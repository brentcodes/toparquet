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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.hadoop.ParquetWriter;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents an output location for data
 */
@Value @AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Partition {
    Object key;
    PartitionType partitionType;
    Partitions subPartitions;
    RecordWriter writer;
    ReentrantLock writeLock;
    //ColumnTransformation partitionBy;

    public enum PartitionType {
        /**
         * Contains subpartition(s)
         */
        superPartition,

        /**
         * Mapped to a parquet file for writing
         */
        terminalPartition
    }

    public static Partition superPartition(Object key) {
        return new Partition(key, PartitionType.superPartition, new Partitions(), null, null);
    }

    public static Partition terminalPartition(Object key, RecordWriter writer) {
        return new Partition(key, PartitionType.terminalPartition, null, writer, new ReentrantLock() );
    }
}
