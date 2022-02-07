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
package outofmemory.toparquet.cmd;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import outofmemory.toparquet.lib.Converter;
import outofmemory.toparquet.lib.config.ConversionConfig;
import outofmemory.toparquet.lib.structure.ClosableChain;
import outofmemory.toparquet.lib.util.Coalescer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

//import static outofmemory.toparquet.lib.util.Coalescer.coalesc;

/**
 * Runs on a single machine and converts csv files to parquet files
 */
public class SingleNodeCommandlineApp {

    @SneakyThrows
    public static void main(String[] args) {
        final Options options = getOptions();
        CommandLine commandLine = new DefaultParser().parse(options, args);

        if (commandLine.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ant", options);
            System.exit(0);
        }

        FileSystem fileSystem = FileSystem.get(new Configuration());

        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
                .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);


        ConversionConfig cmdConfig = new ConversionConfig();
        cmdConfig.setOutputPath(commandLine.getOptionValue("o"));
        cmdConfig.setThreads((Integer) commandLine.getParsedOptionValue("t"));
        cmdConfig.setSchemaPath(commandLine.getOptionValue("s"));

        ConversionConfig cmdJsonConfig = commandLine.hasOption("cj") ? mapper.readValue(commandLine.getOptionValue("cj"), ConversionConfig.class) : null;

        String jsonPath = commandLine.hasOption("cp") ?
                     commandLine.getOptionValue("cp") :
                     (fileSystem.exists(new Path("config.json")) ? "config.json" : null);

        ConversionConfig fileJsonConfig = null;
        if (jsonPath != null) {
            fileJsonConfig = mapper.readValue(
                    ClosableChain
                            .builder(() -> fileSystem.open(new Path(jsonPath)))
                            .alsoUsing(InputStreamReader::new)
                            .alsoUsing(BufferedReader::new)
                            .getValueAndClose(reader -> reader.lines().collect(Collectors.joining("\n")))
                            .getBytes(StandardCharsets.UTF_8),
                    ConversionConfig.class
            );
        }

        ArrayList<Path> inputs = new ArrayList<>();

        if (commandLine.hasOption("ip")) {
            final RemoteIterator<LocatedFileStatus> filesInDirectory = fileSystem.listFiles(new Path(commandLine.getOptionValue("ip")), true);
            while (filesInDirectory.hasNext()) {
                final LocatedFileStatus fileStatus = filesInDirectory.next();
                final Path path = fileStatus.getPath();
                if (path.getName().contains(".csv") || path.getName().contains(".tsv") || path.getName().contains(".psv")) {
                    inputs.add(path);
                }
            }
        }

        if (commandLine.hasOption("if")) {
            inputs.addAll(
                    Arrays.stream(
                        commandLine.getOptionValue("if").replaceAll("( )+", " ").split(" ")
                    )
                    .map(Path::new)
                    .collect(Collectors.toList())
            );
        }

        ConversionConfig finalConfig = Coalescer.coalesce(cmdConfig, Coalescer.coalesce(cmdJsonConfig, Coalescer.coalesce(fileJsonConfig, ConversionConfig.getDefaults())));

        final Converter converter = new Converter(inputs, finalConfig, fileSystem, new Random().nextInt());
        converter.execute();

    }

    static Options getOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print help");
        options.addOption("cp","configPath", true, "Location of json config file");
        options.addOption("cj", "configJson", true, "Json configuration string");
        options.addOption("ip", "inputPath", true, "Path to directory containing csv files to convert");
        options.addOption("if", "inputFiles", true, "Specific file(s) to process");
        options.addOption("o", "outputPath", true, "Where to place the parquet files");
        options.addOption("s", "schema", true, "Path to the parquet schema file used for output");
        options.addOption(Option.builder("t")
                        .hasArg(true)
                        .longOpt("threads")
                        .type(Integer.class)
                        .desc("Number of threads dedicated to execution of conversion")
                        .build()
        );
        return options;
    }



}
