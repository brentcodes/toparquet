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
package outofmemory.toparquet.lib.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * Provides an interface to easily interact with an object which is synchronized via a ReadWriteLock
 * @param <ReadOps>
 *     Interface which only contains read operations. No methods which modify state should be included.
 * @param <ReadWriteOps>
 *     Interface which provides access to all of the object's methods.
 */
public abstract class ReadWriteLocked<ReadOps, ReadWriteOps> {
     final protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

     protected abstract ReadOps getReadOperations();

     protected abstract ReadWriteOps getReadWriteOperations();

    /**
     * Provides a thread safe way to return a value from an object.
     *  WARNING: Returning any reference to the Ops object violates the thread-safety of this object.
     */
     public <T> T readValue(Function<ReadOps, T> getter) {
         try {
             lock.readLock().lock();
             final ReadOps readOperations = getReadOperations();
             final T value = getter.apply(readOperations);
             if (readOperations == value)
                 throw new RuntimeException("Returning the ops violates the thread-safety of " + this);
             return value;
         }
         finally {
             lock.readLock().unlock();
         }
     }

    /**
     * Provides a thread safe way to return a value from an object, and create it if it does not exist already.
     *  WARNING: Returning any reference to the Ops object violates the thread-safety of this object.
     * @param getter
     *      Attempts to return a value; only read operations are permitted
     * @param whenNull
     *      Executed when the getter returns null. This method is expected to create the missing value, return it,
     *          and add it to the object if desired.
     */
     public <T> T readValue(Function<ReadOps, T> getter, Function<ReadWriteOps, T> whenNull) {
         T value = readValue(getter);
         if (value == null) {
             try {
                 lock.writeLock().lock();
                 value = getter.apply(getReadOperations());

                 if (value == null) {
                     ReadWriteOps readWriteOps = getReadWriteOperations();
                     value = whenNull.apply(readWriteOps);
                     if (readWriteOps == value)
                         throw new RuntimeException("Returning the ops violates the thread-safety of " + this);
                 }
             }
             finally {
                 lock.writeLock().unlock();
             }
         }
         return value;
     }

}
