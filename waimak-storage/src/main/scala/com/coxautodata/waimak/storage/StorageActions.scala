package com.coxautodata.waimak.storage

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import com.coxautodata.waimak.dataflow.ActionResult
import com.coxautodata.waimak.dataflow.spark.{SimpleAction, SparkDataFlow}
import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

import scala.util.{Failure, Success, Try}

/**
  * Created by Vicky Avison on 11/05/18.
  */
object StorageActions extends Logging {


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
      * Writes a Dataset to to the storage layer
      *
      * @param labelName      the label whose Dataset we wish to write
      * @param table          the AuditTable object to use for writing
      * @param lastUpdatedCol the last updated column in the Dataset
      * @param appendDateTime timestamp of the append, zoned to a timezone
      * @param doCompaction   a lambda used to decide whether a compaction should happen after an append.
      *                       Takes list of table regions, the count of records added in this batch and
      *                       the compaction zoned date time.
      *                       Default is not to trigger a compaction.
      * @return a new SparkDataFlow with the write action added
      */
    def writeToStorage(labelName: String, table: AuditTable, lastUpdatedCol: String, appendDateTime: ZonedDateTime,
                       doCompaction: (Seq[AuditTableRegionInfo], Long, ZonedDateTime) => Boolean = (_, _, _) => false): SparkDataFlow = {
      val run: Map[String, Dataset[_]] => ActionResult[Dataset[_]] = m => {
        import sparkDataFlow.flowContext.spark.implicits._

        val appendTimestamp = Timestamp.valueOf(appendDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime)

        table.append(m(labelName), $"$lastUpdatedCol", appendTimestamp) match {
          case Success((t, c)) if doCompaction(t.regions, c, appendDateTime) =>
            logInfo(s"Compaction has been triggered on table [$labelName], with compaction timestamp [$appendTimestamp].")
            t.compact(appendTimestamp) match {
              case Success(_) => Seq.empty
              case Failure(e) => throw StorageException(s"Failed to compact table [$labelName], with compaction timestamp [$appendTimestamp]", e)
            }
          case Success(_) => Seq.empty
          case Failure(e) => throw StorageException(s"Error appending data to table [$labelName], using last updated column [$lastUpdatedCol]", e)
        }
        Seq.empty
      }
      sparkDataFlow.addAction(new SimpleAction(List(labelName), List.empty, run))
    }


    /**
      * Get a snapshot of tables in the storage layer for a given timestamp
      *
      * @param storageBasePath   the base path of the storage layer
      * @param snapshotTimestamp the snapshot timestamp
      * @param includeHot        whether or not to include hot partitions in the read
      * @param tables            the tables we want to snapshot
      * @return a new SparkDataFlow with the snapshot actions added
      */
    def snapshotFromStorage(storageBasePath: String, snapshotTimestamp: Timestamp, includeHot: Boolean = true)(tables: String*): SparkDataFlow = {
      val basePath = new Path(storageBasePath)

      val (existingTables, newTables) = Storage.openFileTables(sparkDataFlow.flowContext.spark, basePath, tables.toSeq, includeHot)
      if (newTables.nonEmpty) throw new RuntimeException(s"Tables do not exist: $newTables")

      handleTableErrors(existingTables, "Unable to perform read")

      existingTables.values.map(_.get).foldLeft(sparkDataFlow)((flow, table) => {
        flow.addAction(new SimpleAction(List.empty, List(table.tableName), _ => List(table.snapshot(snapshotTimestamp))))
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
    def loadFromStorage(storageBasePath: String, from: Option[Timestamp] = None, to: Option[Timestamp] = None)(tables: String*): SparkDataFlow = {
      val basePath = new Path(storageBasePath)

      val (existingTables, newTables) = Storage.openFileTables(sparkDataFlow.flowContext.spark, basePath, tables.toSeq)
      if (newTables.nonEmpty) throw new RuntimeException(s"Tables do not exist: $newTables")

      handleTableErrors(existingTables, "Unable to perform read")

      existingTables.values.map(_.get).foldLeft(sparkDataFlow)((flow, table) => {
        flow.addAction(new SimpleAction(List.empty, List(table.tableName), _ => List(table.allBetween(from, to))))
      })
    }

  }

}
