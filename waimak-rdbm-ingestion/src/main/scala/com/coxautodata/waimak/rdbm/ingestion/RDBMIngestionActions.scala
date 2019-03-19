package com.coxautodata.waimak.rdbm.ingestion

import java.sql.Timestamp
import java.time.ZonedDateTime
import java.util.UUID

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow.spark.{SimpleAction, SparkDataFlow}
import com.coxautodata.waimak.dataflow.{ActionResult, DataFlowEntities}
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.storage.StorageActions._
import com.coxautodata.waimak.storage.{AuditTable, AuditTableInfo, AuditTableRegionInfo}

/**
  * Created by Vicky Avison on 08/05/18.
  */
object RDBMIngestionActions {


  implicit class RDBMActionImplicits(sparkDataFlow: SparkDataFlow) extends Logging {

    /**
      * Extracts data from a RDBM into the storage layer.
      * Handles storage initialization for the table if this is the first time we are extracting.
      *
      * @param rdbmExtractor     The RDBMExtractor to use for extraction
      * @param dbSchema          The database schema (namespace) to read from
      * @param storageBasePath   The base path for the storage layer
      * @param tableConfigs      table configuration
      * @param extractDateTime   Datetime of the append, zoned to a timezone, should be the datetime the flow was executed
      * @param lastUpdatedOffset Number of seconds to subtract from the last updated timestamp before reason
      *                          (for safety/contingency)
      * @param forceFullLoad     If set to true, ignore the last updated and read everything
      * @param doCompaction      a lambda used to decide whether a compaction should happen after an append.
      *                          Takes list of table regions, the count of records added in this batch and
      *                          the compaction timestamp.
      *                          Default is not to trigger a compaction
      * @param tables            The tables to read
      * @return A new SparkDataFlow with the extraction actions added
      */
    def extractToStorageFromRDBM(rdbmExtractor: RDBMExtractor
                                 , dbSchema: String
                                 , storageBasePath: String
                                 , tableConfigs: Map[String, RDBMExtractionTableConfig]
                                 , extractDateTime: ZonedDateTime
                                 , lastUpdatedOffset: Long = 0
                                 , forceFullLoad: Boolean = false
                                 , doCompaction: (Seq[AuditTableRegionInfo], Long, ZonedDateTime) => Boolean = (_, _, _) => false)(tables: String*): SparkDataFlow = {

      def metadataFunction(tableName: String): AuditTableInfo = {
        val tableConfig = tableConfigs(tableName)
        rdbmExtractor.getTableMetadata(dbSchema, tableName, tableConfig.pkCols, tableConfig.lastUpdatedColumn, tableConfig.forceRetainStorageHistory).get
      }

      val randomPrefix = UUID.randomUUID().toString

      val openFlow = sparkDataFlow
        .getOrCreateAuditTable(storageBasePath, Some(metadataFunction), Some(randomPrefix), true)(tables: _*)

      tables.foldLeft(openFlow) { (flow, tableName) =>
        flow
          .extractFromRDBM(rdbmExtractor, lastUpdatedOffset, tableName, randomPrefix, tableConfigs(tableName).maxRowsPerPartition, forceFullLoad)
          .writeToStorage(tableName, rdbmExtractor.rdbmRecordLastUpdatedColumn, extractDateTime, doCompaction, randomPrefix)
      }

    }

    /**
      * Extract a table from a RDBM
      *
      * @param rdbmExtractor         The RDBMExtractor to use for extraction
      * @param lastUpdatedOffset     Number of seconds to subtract from the last updated timestamp before reason
      *                              (for safety/contingency)
      * @param label                 The waimak label for this table
      * @param auditTableLabelPrefix the prefix of the audit table entity on the flow. The AuditTable will be
      *                              found with `s"${auditTableLabelPrefix}_$label"`
      * @param maxRowsPerPartition   Optionally, the maximum number of rows to be read per Dataset partition
      * @param forceFullLoad         If set to true, ignore the last updated and read everything
      * @return A new SparkDataFlow with the extraction actions added
      */
    def extractFromRDBM(rdbmExtractor: RDBMExtractor
                        , lastUpdatedOffset: Long
                        , label: String
                        , auditTableLabelPrefix: String
                        , maxRowsPerPartition: Option[Int] = None
                        , forceFullLoad: Boolean = false): SparkDataFlow = {

      val auditTableLabel = s"${auditTableLabelPrefix}_$label"

      val run: DataFlowEntities => ActionResult = {
        m =>
          val auditTable = m.get[AuditTable](auditTableLabel)
          val lastUpdated = auditTable.getLatestTimestamp()
            .map(_.toLocalDateTime.minusSeconds(lastUpdatedOffset))
            .map(Timestamp.valueOf)

          logInfo(s"Extracting table $label with last updated $lastUpdated and metadata ${auditTable.meta}")


          Seq(Some(rdbmExtractor.getTableDataset(auditTable.meta, lastUpdated, maxRowsPerPartition, forceFullLoad)))
      }

      sparkDataFlow.addAction(new SimpleAction(List(auditTableLabel), List(label), run, "extractFromRDBM"))
    }


    /**
      * For SQlServer temporal tables, using [[com.coxautodata.waimak.storage.AuditTable.snapshot(ts)]] to snapshot will not work due
      * to the extra complication of resolving the main and history tables to find delete events.
      * To correctly snapshot a temporal table, for each pk we need to:
      * 1. order by start col desc, end col asc
      * 2. take the first record in this ordering IF the snapshot timestamp is between the start col and the end col,
      * otherwise return no record for this pk (this record has been deleted)
      *
      * @param storageBasePath   The base path for the storage layer
      * @param snapshotTimestamp The snapshot timestamp
      * @param tables            the tables we wish to read
      * @return A new SparkDataFlow with the read actions added
      */
    def snapshotTemporalTablesFromStorage(storageBasePath: String, snapshotTimestamp: Timestamp)(tables: String*): SparkDataFlow = {

      def run(auditTableLabel: String): DataFlowEntities => ActionResult = m => {
        val table = m.get[AuditTable](auditTableLabel)
        val temporalTableMetadata = CaseClassConfigParser.fromMap[SQLServerTemporalTableMetadata](table.meta)
        if (!temporalTableMetadata.isTemporal) Seq(table.snapshot(snapshotTimestamp))
        else Seq(table.allBetween(None, Some(snapshotTimestamp))
          .map(ds => RDBMIngestionUtils.snapshotTemporalTableDataset(ds, snapshotTimestamp, temporalTableMetadata)))
      }

      val randomPrefix = UUID.randomUUID().toString

      val openFlow = sparkDataFlow
        .getOrCreateAuditTable(storageBasePath, None, Some(randomPrefix), true)(tables: _*)

      tables.foldLeft(openFlow)((flow, table) => {
        val auditTableLabel = s"${randomPrefix}_$table"
        flow.addAction(new SimpleAction(List(auditTableLabel), List(table), run(auditTableLabel), "snapshotTemporalTablesFromStorage"))
      })

    }
  }

}
