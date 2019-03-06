package com.coxautodata.waimak.storage

import java.sql.Timestamp
import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.UUID

import com.coxautodata.waimak.dataflow.spark.{SimpleAction, SparkDataFlow}
import com.coxautodata.waimak.dataflow.{ActionResult, DataFlowEntities}
import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

import scala.util.{Failure, Success, Try}

/**
  * Created by Vicky Avison on 11/05/18.
  */
object StorageActions extends Logging {

  val storageParamPrefix: String = "spark.waimak.storage"
  /**
    * Maximum age of old region files kept in the .Trash folder
    * after a compaction has happened.
    */
  val TRASH_MAX_AGE_MS: String = s"$storageParamPrefix.trashMaxAgeMs"
  val TRASH_MAX_AGE_MS_DEFAULT: Long = 86400000
  /**
    * The row number threshold to use for determining small regions to be compacted.
    */
  val SMALL_REGION_ROW_THRESHOLD: String = s"$storageParamPrefix.smallRegionRowThreshold"
  val SMALL_REGION_ROW_THRESHOLD_DEFAULT: Long = 50000000
  /**
    * Approximate maximum number of bytes to be in each hot partition file.
    * Adjust this to control output file size
    */
  val HOT_BYTES_PER_PARTITION = s"$storageParamPrefix.hotBytesPerPartition"
  val HOT_BYTES_PER_PARTITION_DEFAULT: Long = 100000000
  /**
    * Approximate maximum number of bytes to be in each cold partition file.
    * Adjust this to control output file size
    */
  val COLD_BYTES_PER_PARTITION = s"$storageParamPrefix.coldBytesPerPartition"
  val COLD_BYTES_PER_PARTITION_DEFAULT: Long = 250000000
  /**
    * Whether to recompact all regions regardless of size (i.e. ignore [[SMALL_REGION_ROW_THRESHOLD]])
    */
  val RECOMPACT_ALL = s"$storageParamPrefix.recompactAll"
  val RECOMPACT_ALL_DEFAULT = false


  def handleTableErrors(tableResults: Map[String, Try[Any]], errorMessageBase: String): Unit = {
    val tableErrors = tableResults.filter(_._2.isFailure).mapValues(_.failed.get)
    if (tableErrors.nonEmpty) {
      tableErrors.foreach(kv => logError(s"$errorMessageBase for table ${kv._1}", kv._2))
      throw new RuntimeException(s"$errorMessageBase for tables: ${tableErrors.keySet}")
    }
  }

  /**
    * Compaction trigger that triggers a compaction after an append only if there are hot partitions,
    * the compaction timestamp is between a given window of hours and no other compaction has happened in that specific window
    * You can have a window that spans a day boundary, i.e. `windowStartHours=22` and `windowEndHours=03`
    *
    * @param windowStartHours the hour of the day at which the window opens
    * @param windowEndHours   the hour of the day at which the window closes
    * @return
    */
  def runSingleCompactionDuringWindow(windowStartHours: Int, windowEndHours: Int): (Seq[AuditTableRegionInfo], Long, ZonedDateTime) => Boolean = {

    (regions, _, compactionDateTime) =>

      // Get first time at windowEndHours after compaction timestamp and
      // then get first time at windowStartHours before end of the window
      val maybeEnd = compactionDateTime.withHour(windowEndHours).withMinute(0).withSecond(0).withNano(0)
      val end = if (maybeEnd.isBefore(compactionDateTime)) maybeEnd.plusDays(1).withHour(windowEndHours).withMinute(0).withSecond(0).withNano(0) else maybeEnd
      val maybeStart = end.withHour(windowStartHours)
      val start = if (maybeStart.isAfter(end)) maybeStart.plusDays(-1).withHour(windowStartHours).withMinute(0).withSecond(0).withNano(0) else maybeStart


      // Check if any hot regions exist and optionally find the latest cold region
      val hotRegionExists = regions.exists(_.store_type == AuditTableFile.HOT_PARTITION)
      val latestCold = regions.collect { case r if r.store_type == AuditTableFile.COLD_PARTITION => r.created_on.toLocalDateTime.atZone(ZoneOffset.UTC) }.sortWith(_.isBefore(_)).reverse.headOption

      if (compactionDateTime.isBefore(start)) {
        logInfo(s"Current timestamp [$compactionDateTime] is not in compaction window therefore will not compact. Next window is: [$start until $end]")
        false
      }
      else if (!hotRegionExists) {
        logInfo(s"In compaction window [$start until $end], however not hot region exists therefore skipping compaction")
        false
      } else if (latestCold.nonEmpty && !latestCold.get.isBefore(start) && !latestCold.get.isAfter(end)) {
        logInfo(s"In compaction window [$start until $end], however a compaction was already done in the window at: ${latestCold.get}")
        false
      } else {
        logInfo(s"Valid compaction in the window [$start until $end], a compaction will be triggered.")
        true
      }
  }

  implicit class StorageActionImplicits(sparkDataFlow: SparkDataFlow) {

    /**
      * Opens or creates a storage layer table and adds the [[AuditTable]] object to the flow with a given label.
      * This can then be used with the [[writeToStorage]] action.
      * Creates a table if it does not already exist in the storage layer and the optional `metadataRetrieval`
      * function is given.
      * Fails if the table does not exist in the storage layer and the optional `metadataRetrieval`
      * function is not given.
      *
      * @param storageBasePath   the base path of the storage layer
      * @param metadataRetrieval an optional function that generates table metadata from a table name.
      *                          This function is used during table creation if a table does not exist in the storage
      *                          layer
      * @param labelPrefix       optionally prefix the output label for the AuditTable.
      *                          If set, the label of the AuditTable will be `s"${labelPrefix}_$table"`
      * @param includeHot        whether or not to include hot partitions in the read
      * @param tableNames        the tables we want to open in the storage layer
      * @return a new SparkDataFlow with the get action added
      */
    def getOrCreateAuditTable(storageBasePath: String,
                              metadataRetrieval: Option[String => AuditTableInfo] = None,
                              labelPrefix: Option[String] = Some("audittable"),
                              includeHot: Boolean = true)(tableNames: String*): SparkDataFlow = {

      val run: DataFlowEntities => ActionResult = _ => {

        val basePath = new Path(storageBasePath)

        val (existingTables, missingTables) = Storage.openFileTables(sparkDataFlow.flowContext.spark, basePath, tableNames, includeHot)

        if (missingTables.nonEmpty && metadataRetrieval.isEmpty) {
          throw StorageException(s"The following tables were not found in the storage layer and could not be created as no metadata function was defined")
        }

        val createdTables = missingTables.map { tableName =>
          val tableInfo = metadataRetrieval.get.apply(tableName)
          logInfo(s"Creating table ${tableInfo.table_name} with metadata $tableInfo")
          tableName -> Storage.createFileTable(sparkDataFlow.flowContext.spark, basePath, tableInfo)
        }.toMap

        handleTableErrors(createdTables, "Unable to perform create")

        handleTableErrors(existingTables, "Unable to perform read")

        val allTables = (existingTables.values.map(_.get) ++ createdTables.values.map(_.get)).map(t => t.tableName -> t).toMap

        tableNames.map(allTables).map(Some(_))

      }

      val labelNames = tableNames.map(t => labelPrefix.map(p => p + "_" + t).getOrElse(t)).toList

      sparkDataFlow.addAction(new SimpleAction(List.empty, labelNames, run, "getOrCreateAuditTable"))

    }

    /**
      * Opens a storage layer table and adds the [[AuditTable]] object to the flow with a given label.
      * This can then be used with the [[writeToStorage]] action.
      * Fails if the table does not exist in the storage layer.
      *
      * @param storageBasePath the base path of the storage layer
      * @param labelPrefix     optionally prefix the output label for the AuditTable.
      *                        If set, the label of the AuditTable will be `s"${labelPrefix}_$table"`
      * @param includeHot      whether or not to include hot partitions in the read
      * @param tableNames      the tables we want to open in the storage layer
      * @return a new SparkDataFlow with the get action added
      */
    def getAuditTable(storageBasePath: String, labelPrefix: Option[String] = Some("audittable"), includeHot: Boolean = true)(tableNames: String*): SparkDataFlow = {

      sparkDataFlow.getOrCreateAuditTable(storageBasePath, None, labelPrefix, includeHot)(tableNames: _*)

    }

    /**
      * Writes a Dataset to the storage layer. The table must have been already opened on the flow by using either
      * the [[getOrCreateAuditTable]] or [[getAuditTable]] actions.
      *
      * @param labelName             the label whose Dataset we wish to write
      * @param lastUpdatedCol        the last updated column in the Dataset
      * @param appendDateTime        timestamp of the append, zoned to a timezone
      * @param doCompaction          a lambda used to decide whether a compaction should happen after an append.
      *                              Takes list of table regions, the count of records added in this batch and
      *                              the compaction zoned date time.
      *                              Default is not to trigger a compaction.
      * @param auditTableLabelPrefix the prefix of the audit table entity on the flow. The AuditTable will be
      *                              found with `s"${auditTableLabelPrefix}_$labelName"`
      * @return a new SparkDataFlow with the write action added
      */
    def writeToStorage(labelName: String
                       , lastUpdatedCol: String
                       , appendDateTime: ZonedDateTime
                       , doCompaction: (Seq[AuditTableRegionInfo], Long, ZonedDateTime) => Boolean = (_, _, _) => false
                       , auditTableLabelPrefix: String = "audittable"): SparkDataFlow = {

      val auditTableLabel = s"${auditTableLabelPrefix}_$labelName"

      val run: DataFlowEntities => ActionResult = m => {
        val flowContext = sparkDataFlow.flowContext
        val sparkSession = flowContext.spark
        import sparkSession.implicits._

        val recompactAll = flowContext.getBoolean(RECOMPACT_ALL, RECOMPACT_ALL_DEFAULT)
        val appendTimestamp = Timestamp.valueOf(appendDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime)

        val table: AuditTable = m.get[AuditTable](auditTableLabel)

        table.append(m.get[Dataset[_]](labelName), $"$lastUpdatedCol", appendTimestamp) match {
          case Success((t, c)) if recompactAll || doCompaction(t.regions, c, appendDateTime) =>
            logInfo(s"Compaction has been triggered on table [$labelName], with compaction timestamp [$appendTimestamp].")

            val trashMaxAge = Duration.ofMillis(flowContext.getLong(TRASH_MAX_AGE_MS, TRASH_MAX_AGE_MS_DEFAULT))
            val smallRegionRowThreshold = flowContext.getLong(SMALL_REGION_ROW_THRESHOLD, SMALL_REGION_ROW_THRESHOLD_DEFAULT)
            val hotBytesPerPartition = flowContext.getLong(HOT_BYTES_PER_PARTITION, HOT_BYTES_PER_PARTITION_DEFAULT)
            val coldBytesPerPartition = flowContext.getLong(COLD_BYTES_PER_PARTITION, COLD_BYTES_PER_PARTITION_DEFAULT)

            t.compact(compactTS = appendTimestamp
              , trashMaxAge = trashMaxAge
              , coldBytesPerPartition = coldBytesPerPartition
              , hotBytesPerPartition = hotBytesPerPartition
              , smallRegionRowThreshold = smallRegionRowThreshold
              , recompactAll = recompactAll) match {
              case Success(_) => Seq.empty
              case Failure(e) => throw StorageException(s"Failed to compact table [$labelName], with compaction timestamp [$appendTimestamp]", e)
            }
          case Success(_) => Seq.empty
          case Failure(e) => throw StorageException(s"Error appending data to table [$labelName], using last updated column [$lastUpdatedCol]", e)
        }
        Seq.empty
      }
      sparkDataFlow.addAction(new SimpleAction(List(labelName, auditTableLabel), List.empty, run, "writeToStorage"))
    }


    /**
      * Get a snapshot of tables in the storage layer for a given timestamp
      *
      * @param storageBasePath   the base path of the storage layer
      * @param snapshotTimestamp the snapshot timestamp
      * @param includeHot        whether or not to include hot partitions in the read
      * @param outputPrefix      optionally prefix the output label for the Dataset.
      *                          If set, the label of the snapshot Dataset will be `s"${outputPrefix}_$table"`
      * @param tables            the tables we want to snapshot
      * @return a new SparkDataFlow with the snapshot actions added
      */
    def snapshotFromStorage(storageBasePath: String, snapshotTimestamp: Timestamp, includeHot: Boolean = true, outputPrefix: Option[String] = None)(tables: String*): SparkDataFlow = {

      val randomPrefix = UUID.randomUUID().toString
      val openedFlow = sparkDataFlow.getAuditTable(storageBasePath, Some(randomPrefix), includeHot)(tables: _*)

      tables.foldLeft(openedFlow)((flow, table) => {
        val auditTableLabel = s"${randomPrefix}_$table"
        val outputLabel = outputPrefix.map(p => s"${p}_$table").getOrElse(table)
        flow.addAction(new SimpleAction(List(auditTableLabel), List(outputLabel), m => List(m.get[AuditTable](auditTableLabel).snapshot(snapshotTimestamp)), "snapshotFromStorage"))
      })

    }

    /**
      * Load everything between two timestamps for the given tables
      *
      * NB; this will not give you a snapshot of the tables at a given time, it will give you the entire history of
      * events which have occurred between the provided dates for each table. To get a snapshot, use [[snapshotFromStorage]]
      *
      * @param storageBasePath the base path of the storage layer
      * @param from            Optionally, the lower bound last updated timestamp (if undefined, it will read from the beginning of time)
      * @param to              Optionally, the upper bound last updated timestamp (if undefined, it will read up until the most recent events)
      * @param tables          the tables to load
      * @return a new SparkDataFlow with the read actions added
      */
    def loadFromStorage(storageBasePath: String, from: Option[Timestamp] = None, to: Option[Timestamp] = None, includeHot: Boolean = true, outputPrefix: Option[String] = None)(tables: String*): SparkDataFlow = {

      val randomPrefix = UUID.randomUUID().toString
      val openedFlow = sparkDataFlow.getAuditTable(storageBasePath, Some(randomPrefix), includeHot)(tables: _*)

      tables.foldLeft(openedFlow)((flow, table) => {
        val auditTableLabel = s"${randomPrefix}_$table"
        val outputLabel = outputPrefix.map(p => s"${p}_$table").getOrElse(table)
        flow.addAction(new SimpleAction(List(auditTableLabel), List(outputLabel), m => List(m.get[AuditTable](auditTableLabel).allBetween(from, to)), "loadFromStorage"))
      })
    }

  }

}
