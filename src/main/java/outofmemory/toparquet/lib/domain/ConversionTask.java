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

import lombok.Data;
import lombok.Getter;
import lombok.Value;
import org.apache.hadoop.fs.Path;


public abstract class ConversionTask {
     /**
      * A unit of work that involves reading from one file, then writing data to another
      */
     public static class ReadWriteTask extends ConversionTask {
          @Getter
          final Path inputPath;
          @Getter
          final String outputPath;
          @Getter
          final PartitioningStrategy partitioningStrategy;

          public ReadWriteTask(Path inputPath, String outputPath, PartitioningStrategy partitioningStrategy, InputType inputType, OutputType outputType) {
               super(inputType, outputType);
               this.inputPath = inputPath;
               this.outputPath = outputPath;
               this.partitioningStrategy = partitioningStrategy;
          }
     }

     /**
      * A unit of work which makes sure writers are flushed and closed after all data has been written
      */
     public static class CleanUpTask extends ConversionTask {
          @Getter
          final Partition partition;

          public CleanUpTask(Partition partition, InputType inputType, OutputType outputType) {
               super(inputType, outputType);
               this.partition = partition;
          }
     }


     protected ConversionTask(InputType inputType, OutputType outputType) {
          this.inputType = inputType;
          this.outputType = outputType;
     }


     @Getter
     final InputType inputType;
     @Getter
     final OutputType outputType;

     public enum InputType {
          sourceFile(1),
          dedupeIntermediate(2),
          bufferIntermediate(3),
          sortIntermediate(4);

          @Getter
          private final int priority;

          InputType(int priority) {
               this.priority = priority;
          }
     }

     public enum OutputType {
          finalFile,
          bufferIntermediate,
          dedupeIntermediate,
          sortIntermediate
     }
}
