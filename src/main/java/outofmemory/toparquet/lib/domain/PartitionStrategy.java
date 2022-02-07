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

import org.apache.avro.generic.GenericData;

import java.util.function.Function;

import outofmemory.toparquet.lib.domain.Partition.PartitionType;

/**
 * Defines how the records should be partitioned into different output files
 */
public interface PartitionStrategy {
    /**
     * @return
     *      Extracts the key from the record which defines the partition
     */
    Object getPartitionKey(GenericData.Record record, ConversionTask currentTask);

    /**
     * @param partitionKey
     *      The key gotten from getPartitionKey
     * @return
     *      How the partition should be named in the file system
     */
    String getPathValue(Object partitionKey, ConversionTask.ReadWriteTask conversionTask);

    /**
     * @return
     *      The role this partition plays
     */
    PartitionType getPartitionType();

    /**
     * Allow a partition strategy to be implemented by reusing a columnTransformation
     */
    static PartitionStrategy simpleBuild(ColumnTransformation columnTransformation, Function<Object, String> namingFunction, PartitionType partitionType) {
        return new PartitionStrategy() {
            @Override
            public Object getPartitionKey(GenericData.Record record, ConversionTask conversionTask) {
                return columnTransformation.execute(record);
            }

            @Override
            public String getPathValue(Object partitionKey, ConversionTask.ReadWriteTask conversionTask) {
                return namingFunction.apply(partitionKey);
            }

            @Override
            public PartitionType getPartitionType() { return partitionType; }

        };
    }
}
