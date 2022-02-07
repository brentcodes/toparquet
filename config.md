
 [comment]: # ( This file was auto-generated! Do not modify it directly. ) 
### threads

Number of threads to use for processing

*Default Value*:

 The number of CPU cores on the machine

---
### encoding

Text encoding of input files

*Default Value*:

 `UTF8`

---
### hasHeader

Whether the first line of the CSV files are column names

*Default Value*:

 `false`

---
### separator

The delimiter of the input files

*Default Value*:

 

---
### inputCompression

Method of compression used for input files. Use UNCOMPRESSED for no compression

*Default Value*:

 Application will attempt to deduce the compression used

---
### schemaPath

Where to find the output schema

*Default Value*:

 An all String schema will be generated with column names matching the header. If no header is present, they will be assigned names in the spark style ie _c0, _c1, ect

---
### partitionBy

Splits into multiple output files. ***null*** will create a single output partition. 

 WARNING: More partitions means more memory usage

*Default Value*:

 ***null***

---
### outputCompression

What compression to use for the output parquet files

*Default Value*:

 `SNAPPY`

---
### outputPath

Final file name/path

*Default Value*:

 ***null***

---
### backlogLimit

How many records to store if a reader is unable to write to the output parquet file. This can be reduced to lower memory usage at the expense of processing time when there are highly skewed partitions; 0 is an acceptable value

*Default Value*:

 `4096`

---
### useDiskBuffer

If true, the app will write the partitions to disk before attempting to convert them. If false, the partitioning will occur at the same time as conversion.

 False will increase performance drastically, but memory usage will scale with the number of partitions.

*Default Value*:

 Will be set based on the configured partitioning strategy.

---
### workingDirectory

Where to write temp files.

*Default Value*:

 $outputPath/.tmp/

---
### inputErrorTolerance

Whether to terminate if an input file is malformed

*Default Value*:

 `fail`

---
## Sample Json Config 
```json
{
  "configVersion" : "1",
  "threads" : null,
  "encoding" : "UTF8",
  "hasHeader" : false,
  "separator" : ",",
  "inputCompression" : "detect",
  "schemaPath" : null,
  "partitionBy" : null,
  "outputCompression" : "SNAPPY",
  "outputPath" : null,
  "backlogLimit" : 0,
  "useDiskBuffer" : null,
  "workingDirectory" : null,
  "inputErrorTolerance" : "fail"
}
```
