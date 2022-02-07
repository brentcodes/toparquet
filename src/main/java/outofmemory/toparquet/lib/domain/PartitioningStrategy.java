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

import lombok.NoArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import outofmemory.toparquet.lib.config.ConversionConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;


public class PartitioningStrategy extends ArrayList<PartitionStrategy> {

    private PartitioningStrategy() {}

    final static Object singleKey = new Object();

    /**
     * Transforms the user settings into partitions which can be used by the converter
     * @param config
     * @return
     */
    public static PartitioningStrategy fromConfig(ConversionConfig config, Schema schema) {
        final PartitioningStrategy strategies = new PartitioningStrategy();
        if (config.getPartitionBy() == null || config.getPartitionBy().length == 0) {
            strategies.add(singlePartition());
            return strategies;
        }

        for (int i = 0; i < config.getPartitionBy().length; ++i) {
            outofmemory.toparquet.lib.config.Partition partitionSetting = config.getPartitionBy()[i];
            final Partition.PartitionType partitionType = config.getPartitionBy().length - 1 == i ? Partition.PartitionType.terminalPartition : Partition.PartitionType.superPartition;
            List<String> columns = null;
            if ("*".equals(partitionSetting.getColumnName())) {
                columns = schema.getTypes().stream().map(c -> c.getName()).collect(Collectors.toList());
            }
            else if (partitionSetting.getColumnName() == null || "".equals(partitionSetting.getColumnName())) {
                columns = new ArrayList<>(0);
            }
            else {
                columns = java.util.Arrays.asList(partitionSetting.getColumnName());
            }

            ColumnTransformation columnTransformation = null;
            Function<Object, String> naming = null;
            switch (partitionSetting.getType()) {
                case value:
                    columnTransformation = ColumnTransformation.createSimple(columns, ColumnTransformationTypes.getValue);
                    List<String> finalColumns = columns;
                    naming = value -> finalColumns.get(0) + "=" + value;
                    break;
                case random:
                    columnTransformation = ColumnTransformation.createBounded(new ArrayList<>(0), ColumnTransformationTypes.rand, 0, partitionSetting.getNumberOfPartitions());
                    naming = value -> value.toString();
                    break;
                default:
                    throw new RuntimeException(partitionSetting.getType() + " Not Implemented");
            }

            strategies.add(
                PartitionStrategy.simpleBuild(
                        columnTransformation,
                        naming,
                        partitionType
                )
            );
        }
        return strategies;
    }

    public static PartitioningStrategy ofStrategies(PartitionStrategy... strategies) {
        PartitioningStrategy partitioningStrategy = new PartitioningStrategy();
        for (PartitionStrategy s : strategies) {
            partitioningStrategy.add(s);
        }
        return partitioningStrategy;
    }

    private static PartitionStrategy singlePartition() {
        return new PartitionStrategy(){
            @Override
            public Object getPartitionKey(GenericData.Record record, ConversionTask conversionTask) {
                return singleKey;
            }

            @Override
            public String getPathValue(Object partitionKey, ConversionTask.ReadWriteTask conversionTask) {
                return "single-part";
            }

            @Override
            public Partition.PartitionType getPartitionType() {
                return Partition.PartitionType.terminalPartition;
            }
        };
    }

    public static final PartitionStrategy preservePartition() {
        return new PartitionStrategy() {
            @Override
            public Object getPartitionKey(GenericData.Record record, ConversionTask currentTask) {
                return currentTask;
            }

            @Override
            public String getPathValue(Object partitionKey, ConversionTask.ReadWriteTask currentTask) {
                return "";
            }

            @Override
            public Partition.PartitionType getPartitionType() {
                return Partition.PartitionType.terminalPartition;
            }
        };
    }

}
