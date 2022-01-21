package com.coxautodata.waimak.storage

import java.sql.Timestamp
import java.time.{Duration, ZoneOffset, ZonedDateTime}

import com.coxautodata.waimak.dataflow.FlowContext
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.storage.AuditTable.CompactionPartitioner
import com.coxautodata.waimak.storage.StorageActions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  * Contains methods to create tables, open tables.
  *
  * Created by Alexei Perelighin on 2018/04/11
  */
object Storage extends Logging {

  /**
    * Creates file table with default configurations, region ids will Longs with zeros added to the left to make it
    * 20 chars.
    *
    * @param sparkSession
    * @param basePath  parent folder which contains folders with table names
    * @param tableInfo table metadata
    * @return
    * @throws  StorageException : Storage exceptions when:
    *                           1) primary keys are not specified;
    *                           2) the folder already exists
    */
  def createFileTable(sparkSession: SparkSession, basePath: Path
                      , tableInfo: AuditTableInfo): Try[AuditTable] = {
    val fileStorageOps = createFops(sparkSession, basePath)
    val table = AuditTableFile.createTable(sparkSession, fileStorageOps, basePath, tableInfo)(AuditTableFile.nextLongRegion)
    table
  }

  /**
    * Opens multiple tables, reads their metadata and reads the state of the table regions (ids, count of records per region,
    * values of region latest timestamp).
    *
    * You can specify to only read the cold (compacted) state of the table. This is useful to avoid consistency issues during development on tables into which data is currently flowing.
    * This is useful to avoid consistency issues during development on tables into which data is currently flowing.
    *
    * @param sparkSession
    * @param basePath   parent folder which contains folders with table names
    * @param tableNames list of tables to open
    * @param includeHot include freshly appended data that has not been compacted yet. Useful for using production data in development.
    * @return (Map[TABLE NAME, AuditTable], Seq[MISSING TABLES]) - audit table objects that exist and
    *         of table names that were not found under the basePath
    */
  def openFileTables(sparkSession: SparkSession, basePath: Path, tableNames: Seq[String], includeHot: Boolean = true): (Map[String, Try[AuditTable]], Seq[String]) = {
    val fileStorageOps = createFops(sparkSession, basePath)
    val res = AuditTableFile.openTables(sparkSession, fileStorageOps, basePath, tableNames, includeHot)(AuditTableFile.nextLongRegion)
    res
  }

  /**
    * Opens or creates a storage layer table.
    * Creates a table if it does not already exist in the storage layer and the optional `metadataRetrieval`
    * function is given.
    * Fails if the table does not exist in the storage layer and the optional `metadataRetrieval`
    * function is not given.
    *
    * @param sparkSession        Spark Session object
    * @param basePath            Base path of the storage directory
    * @param tableNames          the tables we want to open in the storage layer
    * @param metadataRetrieval   an optional function that generates table metadata from a table name.
    *                            This function is used during table creation if a table does not exist in the storage
    *                            layer or to update the metadata if updateTableMetadata is set to true
    * @param updateTableMetadata whether or not to update the table metadata
    * @param includeHot          whether or not to include hot partitions in the read
    */
  def getOrCreateFileTables(sparkSession: SparkSession,
                            basePath: Path,
                            tableNames: Seq[String],
                            metadataRetrieval: Option[String => AuditTableInfo],
                            updateTableMetadata: => Boolean,
                            includeHot: Boolean = true): Seq[AuditTable] = {

    val (existingTables, missingTables) = Storage.openFileTables(sparkSession, basePath, tableNames, includeHot)

    if (missingTables.nonEmpty && metadataRetrieval.isEmpty) {
      throw StorageException(s"The following tables were not found in the storage layer and could not be created as no metadata function was defined: ${missingTables.mkString(",")}")
    }

    if (updateTableMetadata && metadataRetrieval.isEmpty) {
      throw StorageException(s"$UPDATE_TABLE_METADATA is set to true but no metadata function was defined")
    }

    val existingTablesWithUpdatedMetadata = if (updateTableMetadata) {
      existingTables.mapValues(
        _.flatMap(table => table.updateTableInfo(metadataRetrieval.get.apply(table.tableName)))
        ).toMap
    } else existingTables

    val createdTables = missingTables.map { tableName =>
      val tableInfo = metadataRetrieval.get.apply(tableName)
      logInfo(s"Creating table ${tableInfo.table_name} with metadata $tableInfo")
      tableName -> Storage.createFileTable(sparkSession, basePath, tableInfo)
    }.toMap

    handleTableErrors(createdTables, "Unable to perform create")

    handleTableErrors(existingTables, "Unable to perform read")

    handleTableErrors(existingTablesWithUpdatedMetadata, "Unable to update metadata")

    val allTables = (existingTablesWithUpdatedMetadata.values.map(_.get) ++ createdTables.values.map(_.get)).map(t => t.tableName -> t).toMap

    tableNames.map(allTables)

  }

  /**
    * Writes a Dataset to the storage layer. This function will use various configuration parameters
    * given in the `flowContext` (e.g. recompaction all, compactor to use, trash max age, region row threshold) and
    * call a more specific function below. The more specific function should be used if you want to directly
    * set these configuration parameters in the API yourself.
    *
    * @param flowContext    flow context object
    * @param table          to append data to
    * @param toAppend       dataset to append to the table
    * @param lastUpdatedCol the last updated column in the Dataset
    * @param appendDateTime timestamp of the append, zoned to a timezone
    * @param doCompaction   a lambda used to decide whether a compaction should happen after an append.
    *                       Takes list of table regions, the count of records added in this batch and
    *                       the compaction zoned date time.
    */
  def writeToFileTable(flowContext: FlowContext,
                       table: AuditTable,
                       toAppend: Dataset[_],
                       lastUpdatedCol: String,
                       appendDateTime: ZonedDateTime,
                       doCompaction: CompactionDecision): Unit = {
    val recompactAll = flowContext.getBoolean(RECOMPACT_ALL, RECOMPACT_ALL_DEFAULT)
    val compactionPartitioner = CompactionPartitionerGenerator.getImplementation(flowContext)
    val trashMaxAge = Duration.ofMillis(flowContext.getLong(TRASH_MAX_AGE_MS, TRASH_MAX_AGE_MS_DEFAULT))
    val smallRegionRowThreshold = flowContext.getLong(SMALL_REGION_ROW_THRESHOLD, SMALL_REGION_ROW_THRESHOLD_DEFAULT)
    writeToFileTable(table, toAppend, lastUpdatedCol, appendDateTime, doCompaction, recompactAll, trashMaxAge, smallRegionRowThreshold, compactionPartitioner)
  }

  /**
    * Writes a Dataset to the storage layer. This function allows you to set various parameters
    * directory (e.g. recompaction all, compactor to use, trash max age, region row threshold) and is called
    * by an identical function that takes this information from a Spark context. This specific function should be used if
    * you want to directly set these configuration parameters in the API yourself, however use the less specific function
    * if you wish for these parameters to be handled for you.
    *
    * @param table                   to append data to
    * @param toAppend                dataset to append to the table
    * @param lastUpdatedCol          the last updated column in the Dataset
    * @param appendDateTime          timestamp of the append, zoned to a timezone
    * @param doCompaction            a lambda used to decide whether a compaction should happen after an append.
    *                                Takes list of table regions, the count of records added in this batch and
    *                                the compaction zoned date time.
    * @param recompactAll            Whether to force a recompaction of all regions (expensive)
    * @param trashMaxAge             Maximum age of trashed region before it is deleted
    * @param smallRegionRowThreshold Threshold of a region before it is no longer considered for compaction
    * @param compactionPartitioner   The compaction partitioner to use when performing a compaction
    */
  def writeToFileTable(table: AuditTable,
                       toAppend: Dataset[_],
                       lastUpdatedCol: String,
                       appendDateTime: ZonedDateTime,
                       doCompaction: CompactionDecision,
                       recompactAll: Boolean,
                       trashMaxAge: Duration,
                       smallRegionRowThreshold: Long,
                       compactionPartitioner: CompactionPartitioner): Unit = {
    import toAppend.sparkSession.implicits._

    val appendTimestamp = Timestamp.valueOf(appendDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime)

    table.append(toAppend, $"$lastUpdatedCol", appendTimestamp) match {
      case Success((t, c)) if recompactAll || doCompaction(t.regions, c, appendDateTime) =>
        logInfo(s"Compaction has been triggered on table [${table.tableName}], with compaction timestamp [$appendTimestamp].")
        t.compact(compactTS = appendTimestamp
          , trashMaxAge = trashMaxAge
          , compactionPartitioner = compactionPartitioner
          , smallRegionRowThreshold = smallRegionRowThreshold
          , recompactAll = recompactAll) match {
          case Success(_) => ()
          case Failure(e) => throw StorageException(s"Failed to compact table [${table.tableName}], with compaction timestamp [$appendTimestamp]", e)
        }
      case Success(_) => ()
      case Failure(e) => throw StorageException(s"Error appending data to table [${table.tableName}], using last updated column [$lastUpdatedCol]", e)
    }

  }

  /**
    * Creates File Operations object that is a bridge between the process actions and actual storage and handle
    * write to temp with move to permanent operations.
    *
    * @param sparkSession
    * @param basePath parent folder which contains folders with table names, .tmp and .Trash folders will be underneath.
    * @return
    */
  def createFops(sparkSession: SparkSession, basePath: Path): FileStorageOps = new FileStorageOpsWithStaging(
    FileSystem.get(basePath.toUri(), sparkSession.sparkContext.hadoopConfiguration)
    , sparkSession
    , new Path(basePath, ".tmp")
    , new Path(basePath, ".Trash")
  )
}
