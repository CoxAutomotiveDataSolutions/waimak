package com.coxautodata.waimak.rdbm.ingestion

import java.sql.Timestamp
import java.time.ZonedDateTime

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow.ActionResult
import com.coxautodata.waimak.dataflow.spark.{SimpleAction, SparkDataFlow}
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.storage.StorageActions._
import com.coxautodata.waimak.storage.{AuditTable, AuditTableRegionInfo, Storage}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

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
      * @param lastUpdatedOffset Number of seconds to subtract from the last updated timestamp before reason
      *                          (for safety/contingency)
      * @param forceFullLoad     If set to true, ignore the last updated and read everything
      * @param tables            The tables to read
      * @param doCompaction      a lambda used to decide whether a compaction should happen after an append.
      *                          Takes list of table regions, the count of records added in this batch and
      *                          the compaction timestamp.
      *                          Default is not to trigger a compaction.
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

      val basePath = new Path(storageBasePath)

      val (existingTables, newTables) = Storage.openFileTables(sparkDataFlow.flowContext.spark, basePath, tables.toSeq)

      val newTableMetadata = newTables.map(t => {
        val tableConfig = tableConfigs(t)
        (t,
          rdbmExtractor.getTableMetadata(dbSchema, t, tableConfig.pkCols, tableConfig.lastUpdatedColumn)
            .map(_.copy(table_name = t)))
      }).toMap

      handleTableErrors(newTableMetadata, "Unable to fetch metadata")

      val createdTables = newTableMetadata.values.map(_.get).map(tableInfo => {
        logInfo(s"Creating table ${tableInfo.table_name} with metadata $tableInfo")
        tableInfo.table_name -> Storage.createFileTable(sparkDataFlow.flowContext.spark, basePath, tableInfo)
      }).toMap

      handleTableErrors(createdTables, "Unable to perform create")

      handleTableErrors(existingTables, "Unable to perform read")

      val allTables = existingTables.values.map(_.get) ++ createdTables.values.map(_.get)

      allTables.foldLeft(sparkDataFlow)((flow, table) => {
        val lastUpdatedToUseOnRead = table.getLatestTimestamp()
          .map(_.toLocalDateTime.minusSeconds(lastUpdatedOffset))
          .map(Timestamp.valueOf)

        logInfo(s"Extracting table ${table.tableName} with last updated $lastUpdatedToUseOnRead and metadata ${table.meta}")

        flow.extractFromRDBM(rdbmExtractor
          , table.meta
          , lastUpdatedToUseOnRead
          , table.tableName
          , tableConfigs(table.tableName).maxRowsPerPartition
          , forceFullLoad)
          .writeToStorage(table.tableName, table, rdbmExtractor.rdbmRecordLastUpdatedColumn, extractDateTime, doCompaction)
      })
    }

    /**
      * Extract a table from a RDBM
      *
      * @param rdbmExtractor       The RDBMExtractor to use for extraction
      * @param tableMetadata       Table metadata
      * @param lastUpdated         The last updated timestamp from which we wish to read data (if None, then we read everything)
      * @param label               The waimak label for this table
      * @param maxRowsPerPartition Optionally, the maximum number of rows to be read per Dataset partition
      * @param forceFullLoad       If set to true, ignore the last updated and read everything
      * @return A new SparkDataFlow with the extraction actions added
      */
    def extractFromRDBM(rdbmExtractor: RDBMExtractor
                        , tableMetadata: Map[String, String]
                        , lastUpdated: Option[Timestamp]
                        , label: String
                        , maxRowsPerPartition: Option[Int] = None
                        , forceFullLoad: Boolean = false): SparkDataFlow = {

      val run: Map[String, Dataset[_]] => ActionResult[Dataset[_]] =
        _ => Seq(Some(rdbmExtractor.getTableDataset(tableMetadata, lastUpdated, maxRowsPerPartition, forceFullLoad)))

      sparkDataFlow.addAction(new SimpleAction(List.empty, List(label), run))
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
      val basePath = new Path(storageBasePath)

      val (existingTables, newTables) = Storage.openFileTables(sparkDataFlow.flowContext.spark, basePath, tables.toSeq)
      if (newTables.nonEmpty) throw new RuntimeException(s"Tables do not exist: $newTables")

      handleTableErrors(existingTables, "Unable to perform read")


      def run(table: AuditTable): Map[String, Dataset[_]] => ActionResult[Dataset[_]] = _ => {
        val temporalTableMetadata = CaseClassConfigParser.fromMap[SQLServerTemporalTableMetadata](table.meta)
        if (!temporalTableMetadata.isTemporal) Seq(table.snapshot(snapshotTimestamp))
        else Seq(table.allBetween(None, Some(snapshotTimestamp))
          .map(ds => RDBMIngestionUtils.snapshotTemporalTableDataset(ds, snapshotTimestamp, temporalTableMetadata)))
      }

      existingTables.values.map(_.get).foldLeft(sparkDataFlow)((flow, table) => {
        flow.addAction(new SimpleAction(List.empty, List(table.tableName), run(table)))
      })
    }
  }

}
