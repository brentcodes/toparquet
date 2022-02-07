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
package outofmemory.toparquet.lib;

import com.opencsv.*;
import com.opencsv.exceptions.CsvMalformedLineException;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.thirdparty.com.google.errorprone.annotations.Var;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import outofmemory.toparquet.lib.config.ConversionConfig;
import outofmemory.toparquet.lib.config.PartitionType;
import outofmemory.toparquet.lib.domain.*;
import outofmemory.toparquet.lib.structure.ClosableChain;
import outofmemory.toparquet.lib.structure.TaskQueue;
import outofmemory.toparquet.lib.util.StringParser;
import outofmemory.toparquet.lib.domain.ConversionTask.*;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Converter {

    private String workingDir;

    /**
     * @param inputFiles
     *      The CSV files to convert to parquet
     * @param config
     *      Options for how to convert the files
     * @param fileSystem
     *      Hadoop file system configuration
     */
    public Converter(Collection<Path> inputFiles, ConversionConfig config, FileSystem fileSystem, int executionId) {
        this.inputFiles = inputFiles.stream().collect(Collectors.toList());
        this.config = config;
        this.fileSystem = fileSystem;
        this.executionId = executionId;
    }

    private boolean hasStarted = false;
    private final Partitions rootPartitions = new Partitions();
    private final Partitions bufferPartitions = new Partitions();
    //private final TaskQueue<ConversionTask> conversionTasks = new TaskQueue<>();
    private final ConversionTasks allTasks = new ConversionTasks();
    private final ArrayList<Thread> workers = new ArrayList<>();
    private final List<Path> inputFiles;
    private final ConversionConfig config;
    private final FileSystem fileSystem;
    private final CompressorStreamFactory compressorStreamFactory = new CompressorStreamFactory();
    private final int executionId;
    private final int instanceId = new Random().nextInt();
    private Schema schema;

    /**
     * Starts the conversion process
     */
    @SneakyThrows
    public synchronized void execute() {
        if (hasStarted) throw new RuntimeException("execute has already been called");
        hasStarted = true;

        int numberOfThreads = config.getThreads() == null ? Runtime.getRuntime().availableProcessors() : config.getThreads();
        if (numberOfThreads > inputFiles.size())
            numberOfThreads = inputFiles.size();
        if (numberOfThreads < 1)
            numberOfThreads = 1;

        setup(inputFiles.stream().findFirst().get());
        final PartitioningStrategy partitioningStrategy = PartitioningStrategy.fromConfig(config, schema);


        final boolean useDiskBuffer = config.getUseDiskBuffer() == null ? config.getPartitionBy() == null : config.getUseDiskBuffer();
        final boolean workingDirSet = config.getWorkingDirectory() != null;
        workingDir = workingDirSet ?
                  config.getWorkingDirectory() : config.getOutputPath() + "/.tmp";
        if (!workingDirSet) {
            workingDir = workingDir.replace("//", "/");
        }

        // Set up the tasks which we need to execute
        final TaskQueue<ConversionTask> conversionTasks = allTasks.getQueue(ConversionTask.InputType.sourceFile);
        inputFiles.stream().forEach(inputFilePath -> {
            ConversionTask task = null;
            if (useDiskBuffer)
                task = new ReadWriteTask(inputFilePath, workingDir + "/buffer", partitioningStrategy, ConversionTask.InputType.sourceFile, ConversionTask.OutputType.bufferIntermediate);
            else
                task = new ReadWriteTask(inputFilePath, config.getOutputPath(), partitioningStrategy, ConversionTask.InputType.sourceFile, ConversionTask.OutputType.finalFile);;
            conversionTasks.addTask(task);
        });

        // Set up the worker threads
        for (int i = 0; i < numberOfThreads; ++i) {
            Thread workerThread = new Thread(this::runWorkerThread);
            workerThread.setName("FileConverter:" + i);
            workers.add(workerThread);
            workerThread.run();
        }

        while (conversionTasks.size() > 0) {

        }

        fileSystem.createNewFile(new Path(config.getOutputPath() + "/_SUCCESS"));
    }

    /**
     * Starts a worker thread which will check the queue for task items
     */
    @SneakyThrows
    private void runWorkerThread() {
        while (allTasks.size() > 0) {
            final TaskQueue<ConversionTask> conversionTasks = allTasks.nextQueue();
            if (!conversionTasks.tryTask(this::executeTask, false))
                Thread.sleep(1000);
        }
    }

    /**
     * The root of executing a single unit of work; for now, this is reading in an input file and placing it in partitions
     */
    @SneakyThrows
    private void executeTask(ConversionTask conversionTask) {
        if (conversionTask instanceof ReadWriteTask) {
            final ReadWriteTask readWriteTask = (ReadWriteTask)conversionTask;
            createCsvReader(readWriteTask, false)
                    .executeAndClose(reader -> executeReadWriteTask(readWriteTask, reader));
        }
        else if (conversionTask instanceof CleanUpTask) {
            final CleanUpTask cleanUpTask = (CleanUpTask)conversionTask;
            executeCleanupTask(cleanUpTask);
        }
        else {
            throw new RuntimeException("Not Implemented");
        }
    }

    /**
     * The root of executing a single unit of work; for now, this is reading in an input file and placing it in partitions
     * @param conversionTask
     *      The task being worked on
     * @param reader
     *      A reader for the input csv which will be converted to parquet
     */
    @SneakyThrows
    private void executeReadWriteTask(ReadWriteTask conversionTask, CSVReader reader) {
        final LinkedList<Backlog> writeBacklog = new LinkedList<>();
        final ArrayList<Path> newFilesCreated = new ArrayList<>();
        final List<Partition> newPartitionsCreated = new ArrayList<>();

        Partitions partitions = conversionTask.getOutputType() == ConversionTask.OutputType.bufferIntermediate ? bufferPartitions : rootPartitions;

        while (true) {
            String[] inputRow = null;

            try {
                inputRow = reader.readNext();
            }
            catch (CsvMalformedLineException e){
                if (conversionTask.getInputType() == InputType.sourceFile) {
                    if (config.getInputErrorTolerance() == InputErrorTolerance.ignoreBadRow) {
                        continue;
                    }
                }
                throw e;
            }

            if (inputRow == null)
                break;

            // Create the record
            GenericData.Record record = new GenericData.Record(schema);
            for (Schema.Field field : schema.getFields()) {
                final String value = field.pos() > inputRow.length - 1 ? null : inputRow[field.pos()];
                record.put(field.name(), StringParser.parse(value, field.schema()));
            }

            String output = conversionTask.getOutputPath();

            // Travel down the partition tree until we find the file we want to write to
            for (final PartitionStrategy partitionStrategy : conversionTask.getPartitioningStrategy()) {
                final Object partitionKey = partitionStrategy.getPartitionKey(record, conversionTask);
                final String partitionAsString = partitionStrategy.getPathValue(partitionKey, conversionTask);

                if (partitionStrategy.getPartitionType() == Partition.PartitionType.terminalPartition) {
                    if (partitionAsString != null && !partitionAsString.equals("")) {
                        output += "/" + partitionAsString;
                    }
                    output += "/part-" + Integer.toHexString(executionId) + "-" + Integer.toHexString(instanceId);
                    if (conversionTask.getOutputType() == OutputType.finalFile) {
                        output += "." + config.getOutputCompression().toLowerCase() + ".parquet";
                    }
                    else {
                        output += ".csv";
                    }
                }
                else if (partitionStrategy.getPartitionType() == Partition.PartitionType.superPartition) {
                    output += "/" + partitionAsString;
                }
                final String finalOutput =  output;

                // Get or create the partition this record belongs to
                final Partition partition = partitions.readValue(
                        // Try to get the partition if it already exists
                        partitionMap -> partitionMap.get(partitionKey),
                        // No existing partition for this value, create a new one and add it to the map
                        partitionMap -> {
                            Partition newPartition = null;
                            if (partitionStrategy.getPartitionType() == Partition.PartitionType.terminalPartition) {
                                newPartition = Partition.terminalPartition(
                                        partitionKey,
                                        conversionTask.getOutputType() == ConversionTask.OutputType.finalFile ?
                                                createParquetWriter(finalOutput) : createCSVWriter(finalOutput)
                                );
                                newFilesCreated.add(new Path(finalOutput));
                            }
                            else if (partitionStrategy.getPartitionType() == Partition.PartitionType.superPartition) {
                                newPartition = Partition.superPartition(partitionKey);
                            }
                            partitionMap.put(partitionKey, newPartition);
                            newPartitionsCreated.add(newPartition);
                            return newPartition;
                        }
                );

                if (Partition.PartitionType.terminalPartition.equals(partition.getPartitionType())) {
                    // Write the record to the partition
                    writeRecord(writeBacklog, partition, record);
                }
                else {
                    // Find the partitions in the subpartitions
                    partitions = partition.getSubPartitions();
                }
            }

        }

        // Write the backlog to disk
        int diskMisses = 0;
        while (!writeBacklog.isEmpty()) {
            final Backlog backlog = writeBacklog.removeLast();
            if (writeRecord(writeBacklog, backlog.partition, backlog.record)) {
                diskMisses = 0;
            }
            else {
                ++diskMisses;
            }
            // Don't waste cpu cycles if there's no work that can be done
            final int PAUSE_THRESHOLD = 100;
            if (diskMisses == PAUSE_THRESHOLD) {
                diskMisses = 0;
                Thread.sleep(10);
            }
        }


        // if new partitions were created, add tasks to clean them (close and flush the files so the next step can read them)
        final TaskQueue<ConversionTask> currentQueue = allTasks.getQueue(conversionTask.getInputType());
        for (Partition newPartition : newPartitionsCreated) {
            currentQueue.addTask(new CleanUpTask(
                newPartition,
                conversionTask.getInputType(),
                conversionTask.getOutputType()
            ));
        }


        // if this is a buffer creation operation, setup the next step to read from the buffer afterwards
        if (conversionTask.getOutputType() == ConversionTask.OutputType.bufferIntermediate) {
            final TaskQueue<ConversionTask> bufferQueue = allTasks.getQueue(ConversionTask.InputType.bufferIntermediate);
            for (Path newFile: newFilesCreated) {
                bufferQueue.addTask(new ReadWriteTask(
                        newFile,
                        // shift the file path from the tmp folder to the output folder
                        newFile.toString()
                                .replace(workingDir, config.getOutputPath())
                                .replace("buffer/", "")
                                .replace(newFile.getName(), ""),
                        PartitioningStrategy.ofStrategies(PartitioningStrategy.preservePartition()),
                        ConversionTask.InputType.bufferIntermediate,
                        ConversionTask.OutputType.finalFile
                ));

            }
        }

    }

    @SneakyThrows
    void executeCleanupTask(CleanUpTask conversionTask) {
        conversionTask.getPartition().getWriter().close();
    }

    /**
     * Writes the record to disk, or to the backlog if another thread is already writing to that partition
     * @param writeBacklog
     *      Where the record will be placed if it cannot be written to disk
     * @param partition
     *      The file to be written to
     * @param record
     *      Data to be written
     * @return
     *      Whether a write to the file occurred
     */
    private boolean writeRecord(LinkedList<Backlog> writeBacklog, Partition partition, GenericData.Record record) throws IOException {
        try {
            // Write if another thread is not already writing
            if  (partition.getWriteLock().tryLock()) {
                partition.getWriter().write(record);
                return true;
            }
            else {
                // If the backlog is full, just wait to write until the other threads are done
                if (writeBacklog.size() >= config.getBacklogLimit()) {
                    partition.getWriteLock().lock();
                    partition.getWriter().write(record);
                    return true;
                }
                // Add the record to the backlog and move on
                else {
                    writeBacklog.addFirst(new Backlog(partition, record));
                    return false;
                }
            }
        } finally {
            if (partition.getWriteLock().isHeldByCurrentThread()) {
                partition.getWriteLock().unlock();
            }
        }
    }

    /**
     * Creates a writer we can use to make output files
     * @param output
     *      file location/name
     */
    @SneakyThrows
    private RecordWriter createParquetWriter(String output) {
        final Path path = new Path(output);
        final ParquetWriter<GenericData.Record> recordWriter = AvroParquetWriter
                .<GenericData.Record>builder(path)
                .withCompressionCodec(CompressionCodecName.fromConf(config.getOutputCompression()))
                .withSchema(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
        return new RecordWriter() {
            @Override
            public void write(GenericData.Record record) throws IOException {
                recordWriter.write(record);
            }

            @Override
            public void close() throws IOException {
                recordWriter.close();
            }
        };
    }

    /**
     * Creates a writer we can use to make intermediate files
     * @param output
     *      file location/name
     */
    @SneakyThrows
    private RecordWriter createCSVWriter(String output) {
        final Path path = new Path(output);
        final FSDataOutputStream stream = fileSystem.create(path);
        final ICSVWriter recordWriter = new CSVWriterBuilder(new OutputStreamWriter(stream))
                .withSeparator(config.getSeparator())
                .build();
        return new RecordWriter() {
            @Override
            public void write(GenericData.Record record) throws IOException {
                String[] r = new String[record.getSchema().getFields().size()];
                for (int i = 0; i < r.length; ++i) {
                    final Object value = record.get(i);
                    if (value == null)
                        r[i] = "";
                    else
                        r[i] = String.valueOf(record.get(i));
                }
                recordWriter.writeNext(r);
            }

            @Override
            public void close() throws IOException {
                recordWriter.flush();
                recordWriter.close();
                stream.close();
            }
        };
    }

    /**
     * Gets a csv reader that we can use get the data from the input file
     */
    private ClosableChain<CSVReader> createCsvReader(ReadWriteTask conversionTask, boolean includeHeader) {
        return createCsvReader(conversionTask.getInputPath(), includeHeader);
    }

    /**
     * Gets a CSV reader that we can use get the data from the input file
     */
    private ClosableChain<CSVReader> createCsvReader(Path inputPath, boolean includeHeader) {
        return ClosableChain
                // Open up the file from hadoop
                .builder(() -> fileSystem.open(inputPath))
                // Put inside buffered stream to get mark feature
                .alsoUsing(BufferedInputStream::new)
                // Figure out if the file is using compression, and if so, decompress it
                .alsoUsing(this::getDecompressedStream)
                // Get a reader which the csv reader can use
                .alsoUsing(decompressedStream -> new InputStreamReader(decompressedStream, config.getEncoding()))
                // Create a reader with the settings we have
                .build(textReader -> {
                    final CSVReaderBuilder readerBuilder = new CSVReaderBuilder(textReader);
                    if (config.getHasHeader() && !includeHeader)
                        readerBuilder.withSkipLines(1);
                    if (config.getSeparator() != null) {
                        readerBuilder.withCSVParser(new CSVParserBuilder()
                                .withSeparator(config.getSeparator())
                                .build()
                        );
                    }
                    return readerBuilder.build();
                });
    }

    /**
     * Converts a compressed input stream into a decompressed input stream
     * @param inputStream
     *      Compressed data, must support mark
     * @return
     *      Decompressed data
     */
    @SneakyThrows
    private InputStream getDecompressedStream(InputStream inputStream) {
        String compression = config.getInputCompression();
        if ("none".equalsIgnoreCase(compression)) {
            compression = "uncompressed";
        }
        if (compression == null || "auto".equalsIgnoreCase(compression) || "detect".equalsIgnoreCase(compression)) {
            try {
                compression = CompressorStreamFactory.detect(inputStream);
            } catch (CompressorException e) {
                compression = "uncompressed";
            }
        }
        return "uncompressed".equalsIgnoreCase(compression) ?
                inputStream :
                compressorStreamFactory.createCompressorInputStream(compression, inputStream);
    }

    /**
     * Creates or loads the schema used for the output parquet files
     * @param exampleFile
     *      a path to a file to deduce the schema from
     */
    @SneakyThrows
    private void setup(Path exampleFile) {
        // load schema if it exists
        if (config.getSchemaPath() != null) {
            final String schemaText = ClosableChain
                    .builder(() -> fileSystem.open(new Path(config.getSchemaPath())))
                    .alsoUsing(InputStreamReader::new)
                    .alsoUsing(BufferedReader::new)
                    .getValueAndClose(reader -> reader.lines().collect(Collectors.joining("\n")));
            schema = new Schema.Parser().parse(schemaText);
        }
        // create a default schema since none was provided
        else {
            String[] header = getFirstRow(exampleFile);
            SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.builder().record("mysteryschema" + header.length).fields();
            for (int i = 0; i < header.length; ++i) {
                final String columnName = config.getHasHeader() ? header[i] : "_c" + i;
                builder = builder.optionalString(columnName);
            }
            schema = builder.endRecord();
        }
    }

    /**
     * Gets the first row of a CSV file
     */
    @SneakyThrows
    private String[] getFirstRow(Path path) {
        ClosableChain<CSVReader> dependencies = createCsvReader(path, true);
        try {
            return dependencies.getValue().readNext();
        }
        finally {
            dependencies.close();
        }
    }

    @Value class Backlog { Partition partition; GenericData.Record record; }
}
