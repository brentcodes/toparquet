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

import java.util.LinkedList;
import java.util.function.Consumer;

public class TaskQueue<T> {
    private final LinkedList<T> waiting = new LinkedList<>();
    private final LinkedList<T> inProgress = new LinkedList<>();
    private boolean hasBeenCleanedUp = false;

    public synchronized boolean getHasBeenCleanedUp() {
        return hasBeenCleanedUp;
    }

    public synchronized void markCleanUpCompleted() {
        this.hasBeenCleanedUp = true;
    }


    public synchronized int size() {
        return waiting.size() + inProgress.size();
    }

    public synchronized void addTask(T task) {
        waiting.addFirst(task);
    }

    public synchronized T nextTask() {
        if (waiting.isEmpty())
            return null;
        T task = waiting.removeLast();
        inProgress.addFirst(task);
        return task;
    }

    public synchronized void markComplete(T task) {
        inProgress.remove(task);
    }

    public boolean tryTask(Consumer<T> taskCode, boolean retryOnFail) {
        final T task = nextTask();
        try {
            if (task != null)
                taskCode.accept(task);
            else
                return false;
        }
        catch (Exception ex) {
            markComplete(task);
            if (retryOnFail) {
                addTask(task);
            }
            throw new RuntimeException(ex);
        }
        markComplete(task);
        return true;
    }

}
