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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.junit.Assert;
import org.junit.Test;
import outofmemory.toparquet.lib.config.ConversionConfig;
import outofmemory.toparquet.lib.config.PartitionType;
import outofmemory.toparquet.lib.domain.Partition;
import outofmemory.toparquet.lib.domain.PartitionStrategy;
import outofmemory.toparquet.lib.domain.PartitioningStrategy;

public class PartitionConfigTests {

    @Test
    public void singlePartition() {
        final Schema schema = SchemaBuilder.builder().record("testing").fields().requiredString("_c0").endRecord();
        ConversionConfig config = ConversionConfig.getDefaults();
        PartitioningStrategy strategies = PartitioningStrategy.fromConfig(config, schema);

        Assert.assertEquals(1, strategies.size());

        PartitionStrategy strategy = strategies.get(0);
        Assert.assertEquals(Partition.PartitionType.terminalPartition, strategy.getPartitionType());

        Record r1 = new Record(schema);
        r1.put("_c0", "a test value");
        Record r2 = new Record(schema);
        r2.put("_c0", "different value");
        Assert.assertEquals(strategy.getPartitionKey(r1, null), strategy.getPartitionKey(r2, null));
    }

    @Test
    public void valuePartitions() {
        final Schema schema = SchemaBuilder.builder().record("testing").fields().requiredString("_c0").requiredString("_c1").requiredString("_c2").endRecord();
        ConversionConfig config = ConversionConfig.getDefaults();
        config.setThreads(1);

        outofmemory.toparquet.lib.config.Partition setting1 = new outofmemory.toparquet.lib.config.Partition();
        setting1.setColumnName("_c0");
        setting1.setType(PartitionType.value);
        outofmemory.toparquet.lib.config.Partition setting2 = new outofmemory.toparquet.lib.config.Partition();
        setting2.setColumnName("_c1");
        setting2.setType(PartitionType.value);

        config.setPartitionBy(new outofmemory.toparquet.lib.config.Partition[]{
            setting1,
            setting2
        });

        PartitioningStrategy strategies = PartitioningStrategy.fromConfig(config, schema);
        Assert.assertEquals(2, strategies.size());

        PartitionStrategy strategy1 = strategies.get(0);
        Assert.assertEquals(Partition.PartitionType.superPartition, strategy1.getPartitionType());
        PartitionStrategy strategy2 = strategies.get(1);
        Assert.assertEquals(Partition.PartitionType.terminalPartition, strategy2.getPartitionType());

        Record record = new Record(schema);
        record.put("_c0", "v1");
        record.put("_c1", "v2");
        record.put("_c2", "v3");

        Assert.assertEquals("v1", strategy1.getPartitionKey(record, null));
        Assert.assertEquals("v2", strategy2.getPartitionKey(record, null));
    }


}
