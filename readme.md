# Features
* **Converts CSV files into Parquet.**
* Creates Partitions

# Usage

**Project Status:** *Dev Testing* - Seems to be working, but needs more testing. Do not use in production.

Designed to run on a Hadoop edge node.

## Examples

`java -jar toparquet.jar -if myfile1.csv myfile2.csv -o myparquet`

`java -jar toparquet.jar -ip folder/mydir/ -o myparquet -s myschema.par`


## More Information

`java -jar toparquet.jar -h`

[Advanced/Json Configuration Guide](config.md)

[Development Readme](src/readme.md)

# Goals
The motivation of this project is to allow an in-memory data processing framework, such as Spark, to be able to process a large file that might not fit into a node's memory if it were a CSV.
In some unfortunate circumstances, spark might not have enough memory to do the conversion itself.

* Ability to quickly and easily convert large files into parquet with **minimal memory usage**.
* Preprocess files to be more easily used by spark processing.

# Non-Goals
* This project does not intend to become a data transformation / processing tool in its own right.


# Feature Roadmap
- [ ] Stable CSV to Parquet Conversion
- [ ] Streaming Conversion via Command Line Pipe
- [ ] Failure Tolerance
- [ ] Add Calculated Columns to Output
- [ ] **Basic** Filtering
- [ ] Explode Records (To deal with high-skew joins)
- [ ] Dedupe Records
- [ ] Isolate Large Columns into their own Files
- [ ] Multiply Readers for Input Bottlenecks
- [ ] Multiply Writers for Output Bottlenecks
- [ ] Array Support
- [ ] Per Partition Ordering
- [ ] Option to use Cloud Object Storage instead of Hadoop 
- [ ] Multi-Node Processing?
- [ ] Other Input Formats?