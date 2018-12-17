# Changelog

## Unreleased

## 2.0 - 2018-12-17

### Added
- Parallel scheduler: By default flows will be executed by the parallel scheduler allowing multiple independent actions to be executed at the same time within the same Spark session
- Added `commit` and `push` actions used to commit to permanent storage on the flow using an implementation of `DataCommitter`. The provided `ParquetDataCommitter` implementation can be used to commit labels to a Hadoop FileSystem in Parquet, and optionally to an Hadoop-based Metastore 
- Added a Hive implementation of the `HadoopDBConnector` trait used for committing DDLs for underlying parquet files. This includes an implementation of the Hive SQL dialect (`trait HiveDBConnector`) and a class that submits Hive queries via `sparkSession.sql` (`HiveSparkSQLConnector`) 
- Added a new action writeHiveManagedTable that creates Hive tables using the saveAsTable on the DataFrameWriter class
- Added `SQLServerExtractor` for ingesting data from older (pre-2016) versions of SQL Server
- Added global configuration parameters for the storage layer
- Added experimental Scala 2.12 support

### Changed
- An exception is now thrown by default if any actions in the flow did not run. This can be overridden with an additional boolean option to the execute method on the executor object
- An exception is now thrown if a flow attempts to register a Spark view with an invalid label name
- Renamed `SimpleSparkDataFlow` to `SparkDataFlow`
- Removed unnecessary flow context on Interceptor API
- All Impala `HadoopDBConnector` classes now take a `SparkFlowContext` object to allow them to work on filesystems other than the one given in `fs.defaultFS`

### Removed
- Removed `stageAndCommitParquet` and `stageAndCommitParquetToDB` actions in favour of the new `commit` and `push` approach

### Fixed
- Fixed issue where reading from the storage layer can fail whilst another job is writing to the storage layer (non-compaction write) due to cached region info being overwritten
- Partitions within regions in the storage layer are now calculated on total size (`columns * rows`) instead of just number of rows. This should reduce partition sizes in the case of wide tables
- Corruption detection for region info cache in the storage layer added in the case of job failures during previous writes
- Optimised Waimak flow validator in the case of large/complex flows
- Upgraded the Scala 2.11 compiler version due to a vulnerability in earlier versions

## 1.5.2 - 2018-10-09

### Added
- Allowing optional output prefix on labels when reading from the storage layer

## 1.5.1 - 2018-08-21

### Added
- Support of custom properties for JDBC connections using the Metastore Utils by passing either a `Properties` object or a `Map` so they can be read securely from a `JCEKS` file

### Removed
- Removed support for Spark 2.0 and Spark 2.1

## 1.5 - 2018-08-13

### Added
- Trash deletion feature in Waimak-Storage that will clean up old region compactions stored in `.Trash`
- Interceptor actions will now show details of the actions they intercepted and actions they intercepted with in the Spark UI

### Fixed
- Single cold partitions will no longer be recompacted into themselves in the storage layer

## 1.4.3 - 2018-08-13

### Fixed
- Azure Table uploader will now clean up thread pools to prevent exhausting system threads after being invoked multiple times

## 1.4.2 - 2018-08-06

### Added
- Added optional Spark parameter `spark.waimak.fs.defaultFS` to specify the URI of the FileSystem object in the [`SparkFlowContext`](src/main/scala/com/coxautodata/waimak/dataflow/spark/SparkFlowContext.scala)

## 1.4.1 - 2018-07-27

### Fixed
- Azure Table uploader now respects and uploads `null` values instead of converting them to zero'd values

## 1.4 - 2018-07-05

### Added
- Better exception logging on failing actions during execution
- `Any` types allowed to be used by and returned from actions
- Impala queries to the same connection object now reuse connections to improve query submission performance

### Fixed
- Spark 2.0, 2.1 and 2.3 compatability

## 1.3.1 - 2018-07-02

### Fixed
- Azure Table writer hanging after API failures
