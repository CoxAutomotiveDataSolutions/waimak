package com.coxautodata.waimak.storage

import java.sql.Timestamp
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

import com.coxautodata.waimak.dataflow.spark.{SimpleAction, SparkDataFlow}
import com.coxautodata.waimak.dataflow.{ActionResult, DataFlowEntities, FlowContext}
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.storage.AuditTable.CompactionPartitioner
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

import scala.util.Try

/**
  * Created by Vicky Avison on 11/05/18.
  */
object StorageActions extends Logging {

  type CompactionDecision = (Seq[AuditTableRegionInfo], Long, ZonedDateTime) => Boolean

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
    * The class name of the compaction partitioner implementation to use
    */
  val COMPACTION_PARTITIONER_IMPLEMENTATION = s"$storageParamPrefix.compactionPartitionerImplementation"
  val COMPACTION_PARTITIONER_IMPLEMENTATION_DEFAULT: String = "com.coxautodata.waimak.storage.TotalBytesPartitioner"
  /**
    * Approximate maximum number of bytes to be in each partition file.
    * Adjust this to control output file size.
    * Use with the [[TotalBytesPartitioner]]
    */
  val BYTES_PER_PARTITION = s"$storageParamPrefix.bytesPerPartition"
  val BYTES_PER_PARTITION_DEFAULT: Long = 250000000
  /**
    * Maximum records to when calculating the number of bytes in a partition.
    * Use -1 for no sampling.
    * Use with the [[TotalBytesPartitioner]]
    */
  val MAX_RECORDS_TO_SAMPLE = s"$storageParamPrefix.maxRecordsToSample"
  val MAX_RECORDS_TO_SAMPLE_DEFAULT: Long = 1000
  /**
    * Approximate maximum number of cells (numRows * numColumns) to be in each cold partition file.
    * Adjust this to control output file size.
    * Use with the [[TotalCellsPartitioner]]
    */
  val CELLS_PER_PARTITION = s"$storageParamPrefix.cellsPerPartition"
  val CELLS_PER_PARTITION_DEFAULT: Long = 2500000
  /**
    * Whether to recompact all regions regardless of size (i.e. ignore [[SMALL_REGION_ROW_THRESHOLD]])
    */
  val RECOMPACT_ALL = s"$storageParamPrefix.recompactAll"
  val RECOMPACT_ALL_DEFAULT = false

  /**
    * Whether to update the metadata (call the metadata retrieval function and persist the result) for existing tables
    */
  val UPDATE_TABLE_METADATA = s"$storageParamPrefix.updateMetadata"
  val UPDATE_TABLE_METADATA_DEFAULT = false


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
      * @param storageBasePath     the base path of the storage layer
      * @param metadataRetrieval   an optional function that generates table metadata from a table name.
      *                            This function is used during table creation if a table does not exist in the storage
      *                            layer or to update the metadata if updateTableMetadata is set to true
      * @param labelPrefix         optionally prefix the output label for the AuditTable.
      *                            If set, the label of the AuditTable will be `s"${labelPrefix}_$table"`
      * @param includeHot          whether or not to include hot partitions in the read
      * @param updateTableMetadata whether or not to update the table metadata. Uses spark.waimak.storage.updateMetadata
      *                            by default (which defaults to false)
      * @param tableNames          the tables we want to open in the storage layer
      * @return a new SparkDataFlow with the get action added
      */
    def getOrCreateAuditTable(storageBasePath: String,
                              metadataRetrieval: Option[String => AuditTableInfo] = None,
                              labelPrefix: Option[String] = Some("audittable"),
                              includeHot: Boolean = true,
                              updateTableMetadata: => Boolean = sparkDataFlow.flowContext.getBoolean(UPDATE_TABLE_METADATA, UPDATE_TABLE_METADATA_DEFAULT))(tableNames: String*): SparkDataFlow = {

      val run: DataFlowEntities => ActionResult = _ => Storage.getOrCreateFileTables(sparkDataFlow.flowContext.spark, new Path(storageBasePath), tableNames, metadataRetrieval, updateTableMetadata, includeHot).map(Some.apply)

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
      sparkDataFlow.getOrCreateAuditTable(storageBasePath, None, labelPrefix, includeHot, updateTableMetadata = false)(tableNames: _*)

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
                       , doCompaction: CompactionDecision = (_, _, _) => false
                       , auditTableLabelPrefix: String = "audittable"): SparkDataFlow = {

      val auditTableLabel = s"${auditTableLabelPrefix}_$labelName"

      val run: DataFlowEntities => ActionResult = m => {

        val table: AuditTable = m.get[AuditTable](auditTableLabel)
        val toAppend: Dataset[_] = m.get[Dataset[_]](labelName)

        Storage.writeToFileTable(sparkDataFlow.flowContext, table, toAppend, lastUpdatedCol, appendDateTime, doCompaction)

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

trait CompactionPartitionerGenerator {
  def getCompactionPartitioner(flowContext: FlowContext): CompactionPartitioner
}

object CompactionPartitionerGenerator {

  import com.coxautodata.waimak.storage.StorageActions._

  def getImplementation(flowContext: FlowContext): CompactionPartitioner = {
    import scala.reflect.runtime.{universe => ru}
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val module = m.staticModule(flowContext.getString(COMPACTION_PARTITIONER_IMPLEMENTATION, COMPACTION_PARTITIONER_IMPLEMENTATION_DEFAULT))
    m.reflectModule(module).instance.asInstanceOf[CompactionPartitionerGenerator].getCompactionPartitioner(flowContext)
  }

}

/**
  * A compaction partitioner that partitions on the
  * approximate maximum number of bytes to be in each partition file
  */
object TotalBytesPartitioner extends CompactionPartitionerGenerator {

  import com.coxautodata.waimak.storage.StorageActions._
  import org.apache.spark.util.SizeEstimator

  override def getCompactionPartitioner(flowContext: FlowContext): CompactionPartitioner = {
    val bytesPerPartition = flowContext.getLong(BYTES_PER_PARTITION, BYTES_PER_PARTITION_DEFAULT)
    val maxRecordsToSample = flowContext.getLong(MAX_RECORDS_TO_SAMPLE, MAX_RECORDS_TO_SAMPLE_DEFAULT)
    (ds: Dataset[_], numRows: Long) => {
      val sampled = {
        if (numRows <= maxRecordsToSample || maxRecordsToSample == -1) ds
        else ds.sample(withReplacement = false, maxRecordsToSample / numRows.toDouble)
      }
      // For spark 3 or scala212 mean no longer returns 0 for mean of empty DF, instead it throws
      // For now we will just return 0 when we throw here
      val averageBytesPerRow: Double = Try(sampled.toDF().rdd.map(SizeEstimator.estimate).mean()).getOrElse(0.0)
      Math.ceil((numRows * averageBytesPerRow) / bytesPerPartition).toInt.max(1)
    }
  }

}

/**
  * A compaction partitioner that partitions on the
  * approximate maximum number of cells (numRows * numColumns) to be in each partition file
  */
object TotalCellsPartitioner extends CompactionPartitionerGenerator {

  import com.coxautodata.waimak.storage.StorageActions._

  override def getCompactionPartitioner(flowContext: FlowContext): CompactionPartitioner = {
    val cellsPerPartition = flowContext.getLong(CELLS_PER_PARTITION, CELLS_PER_PARTITION_DEFAULT)
    (ds: Dataset[_], numRows: Long) => {
      val cellsPerRow = ds.schema.size
      Math.ceil((numRows * cellsPerRow.toDouble) / cellsPerPartition).toInt.max(1)
    }
  }
}