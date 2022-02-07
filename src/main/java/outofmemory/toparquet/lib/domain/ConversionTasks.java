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
package outofmemory.toparquet.lib.domain;

import outofmemory.toparquet.lib.structure.TaskQueue;

import java.util.SortedMap;
import java.util.TreeMap;

public class ConversionTasks {
    private SortedMap<ConversionTask.InputType, TaskQueue<ConversionTask>> tasks = new TreeMap<>((inputType, queue) -> inputType.getPriority());

    public synchronized TaskQueue<ConversionTask> getQueue(ConversionTask.InputType inputType) {
        return tasks.computeIfAbsent(inputType, key -> new TaskQueue<>());
    }

    public synchronized int size() {
        return tasks.values().stream().mapToInt(queue -> queue.size()).sum();
    }

    static final TaskQueue<ConversionTask> EMPTY = null;
    public synchronized TaskQueue<ConversionTask> nextQueue() {
        for (TaskQueue<ConversionTask> q : tasks.values()) {
            if (q.size() > 0) {
                return q;
            }
        }
        return EMPTY;
    }

}
