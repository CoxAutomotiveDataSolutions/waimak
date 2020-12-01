# Changelog

## 2.8.6 - 2020-12-05
- Add spark 3 build and publish against spark 3, including new databricks jar

## 2.8.5 - 2020-09-09

### Fixed
- Fixed a bug in the sql server temporal extractor where it would fail to extract from empty
  tables.

## 2.8.4 - 2020-09-08

### Fixed
- Optimise the sql server temporal extractor further to select the appropriate upper bound timestamp
  when selecting from a history table. This should bring in less rows than before.

## 2.8.3 - 2020-09-04

### Fixed
- Fix an issue with the sql server temporal extractor where rows were never being removed from storage, due to 
  having a column added in error. Also try to improve the history query.

## 2.8.2 - 2020-08-18

### Fixed
- Fixed a bug in the WriteAsNamedFilesAction

## 2.8.1 - 2020-06-17

### Fixed
- Fixed a bug in the sql server temporal extractor, which caused extractions to not be isolated within a transaction

## 2.8 - 2019-12-18

### Changed
- Delimiter for the Deequ `genericSQLCheck` changed from a comma to a semi-colon to allow for checks which have commas in them
- `writeAsNamedFiles` now creates the destination directory before performing the write
- Refactored internal scheduler code to make it more functional

## 2.7 - 2019-08-12

### Added
- Metadata and configuration extension functionality
- `sparkCache` actions
- `cacheasparquet` and `sparkcache` configuration extensions
- `writeAsNamedFile`
- Data quality monitoring with Amazon Deequ

### Changed
- Using F-bounded types on `DataFlow`
- High-level storage layer API exposed outside of actions (in addition to existing actions)
- Spark 2.4.3 now used for Scala 2.12 build (no longer experimental)

### Removed
- waimak-rdbm-export and waimak-azure-table modules 

### Fixed
- Introduced retry mechanism for `PropertyProvider`

## 2.6 - 2019-06-06

### Fixed
- `HiveDBConnector` now works for complex types (e.g `MapType`, `StructType`)
- `HiveEnv` environment cleanup no longer fails if the database does not exist

## 2.5 - 2019-05-29

### Changed
- Can now create or cleanup multiple environments at once in the `EnvironmentManager`

## 2.4.2 - 2019-05-21

### Fixed
- An issue where mutiple `HiveSparkSQLConnector` with different databases were interfering with each other (tables were being committed into each others' databases)

## 2.4.1 - 2019-05-14

### Added
- The `errorOnUnexecutedActions` flag can now be configured in the `WaimakEnv` configuration class in the Waimak-App module

## 2.4 - 2019-05-09

### Added
- An optional Map can now be passed to the case class configuration parser where the parser will look for additional properties not found elsewhere
- Added `withExecutor` and `execute` functions onto DataFlows allowing DataFlows to be executed inline without needing to create an Executor explicitly and call execute

### Changed
- When using the case class configuration parser, the prefix `spark.` will automatically be added to configuration keys when looking in the SparkConf.
If a different prefix is required (e.g. `spark.projectname.`), this can be configured using `spark.waimak.config.sparkConfPropertyPrefix`

### Fixed
- Parallel scheduler no longer hangs due to Fatal exceptions occurring in actions, but fails the application instead

## 2.3.1 - 2019-04-29

### Changed
- Databases now set a location based on `baseDatabaseLocation/databaseName` in the experimental Waimak App Env class

## 2.3 - 2019-04-09

### Changed
- The temporary folder (if given) now gets cleaned up after a flow has been successfully executed. This behaviour can be disabled by setting the configuration property `spark.waimak.dataflow.removeTempAfterExecution` to `false`. In all cases, the directory will not be deleted if flow execution fails.
- Storage compaction no longer performs `hot -> cold` followed by `cold -> cold` compactions, instead compacting all of the hot regions plus the cold regions under the configured row threshold in a single go. This reduces complexity, and removes the additional round of IOPs and Spark stage.

## 2.2 - 2019-03-29

### Added
- Generic mechanism to provide properties when using the `CaseClassConfigParser`. An object extending the `PropertyProviderBuilder` can now be configured using the `spark.waimak.config.propertyProviderBuilderObjects` configuration parameter. The `PropertiesFilePropertyProviderBuilder` and `DatabricksSecretsPropertyProviderBuilder` implementations are provided in Waimak
- Added feature to optionally remove history from storage tables during compaction by setting the `retain_history` flag in the `AuditTableInfo` metadata. For RDBM ingestion actions, if no last updated column is provided in the metadata then history is removed. This can be explicitly disabled by setting `forceRetainStorageHistory` to `Some(true)`
- Added Waimak [configuration parameter](https://github.com/CoxAutomotiveDataSolutions/waimak/wiki/Configuration-Parameters) `spark.waimak.storage.updateMetadata` to force the update of table metadata (e.g. in case of primary key or history retention changed)
- Added new experimental module containing currently unstable features

### Changed
- The force recreate mechanism for recreating tables created using a `HadoopDBConnector` has been moved to the Waimak [configuration parameter](https://github.com/CoxAutomotiveDataSolutions/waimak/wiki/Configuration-Parameters) `spark.waimak.metastore.forceRecreateTables`

### Fixed
- All paths used in DDLs submitted through a `HadoopDBConnector` now use the full FileSystem URI including the scheme

## 2.1.1 - 2019-03-15

### Fixed
- Bug in `TotalBytesPartitioner` and `TotalCellsPartitioner` where 0 partitions were returned

## 2.1 - 2019-03-08

### Added
- Additional definitions of the `commit` and `writePartitionedParquet` actions have been added that now take an integer to repartition by
- Added a generic mechanism for calculating the number of files to generate during a recompaction. The default implementation (`TotalBytesPartitioner`) partitions by the estimated final output size, and an alternative implementation (`TotalCellsPartitioner`) partitions by the number of cells (numRows * numColumns)
- The URI used to create the FileSystem in the `SparkFlowContext` object is now also exposed in the object

### Changed
- Breaking changes to the storage action API to simplify their use. Storage tables are now opened as Waimak actions and are stored as entities on the flow. As a result, the `getOrCreateAuditTable`, `writeToStorage` and `snapshotFromStorage` actions can now be done in the same flow

### Fixed
- The force recompaction flag no longer requires an open compaction window
- Test Jar source artifacts are now generated and deployed

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
- Added optional Spark parameter `spark.waimak.fs.defaultFS` to specify the URI of the FileSystem object in the [`SparkFlowContext`](waimak-core/src/main/scala/com/coxautodata/waimak/dataflow/spark/SparkFlowContext.scala)

## 1.4.1 - 2018-07-27

### Fixed
- Azure Table uploader now respects and uploads `null` values instead of converting them to zero'd values

## 1.4 - 2018-07-05

### Added
- Better exception logging on failing actions during execution
- `Any` types allowed to be used by and returned from actions
- Impala queries to the same connection object now reuse connections to improve query submission performance

### Fixed
- Spark 2.0, 2.1 and 2.3 compatibility

## 1.3.1 - 2018-07-02

### Fixed
- Azure Table writer hanging after API failures
