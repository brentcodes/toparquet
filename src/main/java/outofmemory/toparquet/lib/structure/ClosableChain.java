/*  Copyright 2021 brentcodes

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package outofmemory.toparquet.lib.structure;

import lombok.Getter;
import lombok.SneakyThrows;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Stack;

/**
 * Allows stream/reader/write creation code to be put into one method to keep it out of the way while still allowing
 *  all the creation dependencies to be closed
 * @param <T>
 *     The type of the main object the method wishes to return
 */
public class ClosableChain<T> implements Closeable {
    @Getter
    private final T value;
    private final boolean flush;
    private final Stack<Closeable> creationDependencies;

    public ClosableChain(T value, Stack<Closeable> creationDependencies, boolean flush) {
        this.value = value;
        this.flush = flush;
        this.creationDependencies = creationDependencies;
    }

    @Override
    public void close() throws IOException {
        if (value != null && value instanceof Closeable) {
            close((Closeable)value, flush);
        }
        close(creationDependencies, flush);
    }

    /**
     * Use the value of the chain to perform an action, automatically closing the chain afterwards.
     */
    @SneakyThrows
    public void executeAndClose(ValueConsumer<T> withValue) {
        try {
            withValue.consume(getValue());
        }
        finally {
            close();
        }
    }

    private static void close(Stack<Closeable> creationDependencies) {
        close(creationDependencies, false);
    }

    private static void close(Stack<Closeable> creationDependencies, boolean flush) {
        while (!creationDependencies.isEmpty())
            close(creationDependencies.pop(), flush);
    }

    @SneakyThrows
    private static void close(Closeable closeable, boolean flush) {
        Exception flushException = null;
        try {
            if (closeable != null) {
                try {
                    if (flush && closeable instanceof Flushable) {
                        ((Flushable) closeable).flush();
                    }
                }
                catch (Exception e) {
                    flushException = e;
                }
                finally {
                    closeable.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (flushException != null) {
            throw flushException;
        }
    }

    public interface InitialDependencySupplier<Out extends Closeable> {
        Out supply() throws Exception;
    }

    public interface DependencySupplier<In extends Closeable, Out extends Closeable> {
        Out supply(In dependency) throws Exception;
    }

    public interface ValueSupplier<In extends Closeable, Out> {
        Out supply(In dependency) throws Exception;
    }

    public interface ValueConsumer<T> {
        void consume(T value) throws Exception;
    }

    /**
     * Use the returned object to construct the next object.
     *  It will automatically be closed if an exception happens at any point during construction.
     */
    @SneakyThrows
    public static <In extends Closeable> Builder<In> builder(InitialDependencySupplier<In> withFirstDependency) {
        Stack<Closeable> dependencies = new Stack<Closeable>();
        dependencies.push(withFirstDependency.supply());
        return new Builder<>(dependencies, false);
    }

    /**
     * Use the returned object to construct the next object.
     *  It will automatically be closed if an exception happens at any point during construction.
     *  After successfully created, the chain will automatically flush any Flushable objects in the chain before closing
     */
    @SneakyThrows
    public static <In extends Closeable> Builder<In> flushingBuilder(InitialDependencySupplier<In> withFirstDependency) {
        Stack<Closeable> dependencies = new Stack<Closeable>();
        dependencies.push(withFirstDependency.supply());
        return new Builder<>(dependencies, true);
    }

    public static class Builder<In extends Closeable> {
        private final Stack<Closeable> dependencies;
        private final boolean flush;

        private Builder(Stack<Closeable> dependencies, boolean flush) {
            this.dependencies = dependencies;
            this.flush = flush;
        }

        /**
         * With the previous object in the chain as a parameter, use the returned object to construct the next object.
         *  It will automatically be closed if an exception happens at any point during construction.
         */
        public <Out extends Closeable> Builder<Out> alsoUsing(DependencySupplier<In, Out> nextDependency) {
            try {
                dependencies.push(nextDependency.supply((In)dependencies.peek()));
            }
            catch (Exception ex) {
                close(dependencies);
                throw new RuntimeException(ex);
            }
            return new Builder<>(dependencies, flush);
        }

        /**
         * With the previous object in the chain as a parameter, the returned object will be used as the value in
         *  the ClosableChain. Calling this method create the last link dependency chain.
         */
        public <Out> ClosableChain<Out> build(ValueSupplier<In, Out> withFinalValue) {
            Out value;
            try {
                value = withFinalValue.supply((In)dependencies.peek());
            }
            catch (Exception ex) {
                close(dependencies);
                throw new RuntimeException(ex);
            }
            return new ClosableChain<>(value, dependencies, flush);
        }


        /**
         * Builds the chain, but returns the chain's value instead of the chain itself.
         *  After getting the value, all the dependencies in the chain will automatically be closed.
         */
        @SneakyThrows
        public <Out> Out getValueAndClose(ValueSupplier<In, Out> withFinalValue) {
            final ClosableChain<Out> chain = build(withFinalValue);
            try {
                return chain.getValue();
            }
            finally {
                chain.close();
            }
        }
    }

}
