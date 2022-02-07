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
package outofmemory.toparquet.lib.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import outofmemory.toparquet.lib.domain.InputErrorTolerance;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConversionConfig implements Cloneable {
    private String configVersion = "1";

    @Description(
            value = "Number of threads to use for processing",
            defaultValue = "The number of CPU cores on the machine"
    )
    private Integer threads;

    @Description(value = "Text encoding of input files")
    private String encoding;

    @Description("Whether the first line of the CSV files are column names")
    private Boolean hasHeader;

    @Description(value = "The delimiter of the input files", defaultValue = "")
    private Character separator;

    @Description(
            value = "Method of compression used for input files. Use UNCOMPRESSED for no compression",
            defaultValue = "Application will attempt to deduce the compression used"
    )
    private String inputCompression;

    @Description(
            value = "Where to find the output schema",
            defaultValue = "An all String schema will be generated with column names matching the header. If no header is present, they will be assigned names in the spark style ie _c0, _c1, ect"
    )
    private String schemaPath;

    @Description(
            value = "Splits into multiple output files. ***null*** will create a single output partition. \n\n WARNING: More partitions means more memory usage"
    )
    private Partition[] partitionBy;

    @Description(value = "What compression to use for the output parquet files")
    private String outputCompression;

    @Description("Final file name/path")
    private String outputPath;

    @Description("How many records to store if a reader is unable to write to the output parquet file. This can be reduced to lower memory usage at the expense of processing time when there are highly skewed partitions; 0 is an acceptable value")
    private int backlogLimit;

    @Description(
            value = "If true, the app will write the partitions to disk before attempting to convert them. If false, the partitioning will occur at the same time as conversion.\n\n False will increase performance drastically, but memory usage will scale with the number of partitions.",
            defaultValue = "Will be set based on the configured partitioning strategy."
    )
    private Boolean useDiskBuffer;

    @Description(
            value = "Where to write temp files.",
            defaultValue = "$outputPath/.tmp/"
    )
    private String workingDirectory;

    @Description("Whether to terminate if an input file is malformed")
    private InputErrorTolerance inputErrorTolerance;

    private static final ConversionConfig DEFAULTS = new ConversionConfig(
            "1",
            null,
            "UTF8",
            false,
            null,
            "detect",
            null,
            null,
            CompressionCodecName.SNAPPY.toString(),
            null,
            4096,
            null,
            null,
            InputErrorTolerance.fail
    );

    @SneakyThrows
    public static ConversionConfig getDefaults() {
        return (ConversionConfig)DEFAULTS.clone();
    }
}
