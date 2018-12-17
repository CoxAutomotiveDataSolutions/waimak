package com.coxautodata.waimak.rdbm.ingestion

import java.sql.Timestamp
import java.util.Properties

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.storage.AuditTableInfo
import org.apache.spark.sql.{Column, Dataset, SparkSession}

import scala.util.{Failure, Success, Try}


/**
  * A mechanism for generating Waimak actions to extract data from a SQL Server instance containing temporal tables
  * Tables can be a mixture of temporal and non-temporal - both will be handled appropriately
  *
  * @param sparkSession              the SparkSession
  * @param extraConnectionProperties jdbc properties to use (In addition to username and password)
  */
class SQLServerTemporalExtractor(override val sparkSession: SparkSession
                                 , sqlServerConnectionDetails: SQLServerConnectionDetails
                                 , extraConnectionProperties: Properties = new Properties()) extends SQLServerBaseExtractor(sqlServerConnectionDetails, extraConnectionProperties) with Logging {

  val metadataQuery: String =
    s"""(
       | SELECT SCHEMA_NAME(main.schema_id) as schemaName,
       | main.name AS tableName,
       | SCHEMA_NAME (history.schema_id) as historyTableSchema,
       | history.name as historyTableName,
       | colstart.name as startColName,
       | colend.name as endColName,
       | STRING_AGG(tc.name, ';') as primaryKeys
       | FROM sys.tables main
       |-- Find the corresponding history table
       | left join sys .tables history on main.history_table_id = history.object_id
       | left join sys.periods periods on main.object_id = periods.object_id
       |-- Generated at row start(start date)
       | left join sys.columns colstart on periods.start_column_id = colstart.column_id and colstart.object_id = main.object_id
       |-- Generated at row end(row end)
       | left join sys.columns colend on periods.end_column_id = colend.column_id and colend.object_id = main.object_id
       | inner join sys.indexes i on main.object_id = i.object_id
       | inner join sys.index_columns ic on i.object_id = ic.object_id
       | and i.index_id = ic.index_id
       | inner join sys.columns tc on ic.object_id = tc.object_id
       | and ic.column_id = tc.column_id
       | where i.is_primary_key = 1
       |
       | group by main.schema_id,
       | main.name,
       |  history.schema_id,
       |  history.name,
       |  colstart.name,
       |  colend.name,
       |  main.temporal_type
  ) m""".stripMargin

  lazy val allTableMetadata: Map[String, SQLServerTemporalTableMetadata] = {
    import sparkSession.implicits._
    RDBMIngestionUtils.lowerCaseAll(
      sparkSession.read
        .option("driver", driverClass)
        .jdbc(connectionDetails.jdbcString, metadataQuery, connectionProperties)
    )
      .as[SQLServerTemporalTableMetadata]
      .collect()
      .map(metadata => s"${metadata.schemaName}.${metadata.tableName}" -> metadata).toMap
  }

  override def getTableMetadata(dbSchemaName: String
                                , tableName: String
                                , primaryKeys: Option[Seq[String]] = None
                                , lastUpdatedColumn: Option[String] = None): Try[AuditTableInfo] = {
    lastUpdatedColumn.foreach(col => logWarning(
      s"Ignoring user-passed value for last updated ($col) " +
        s"as we can get this information from the database"))

    Try(allTableMetadata(s"$dbSchemaName.$tableName"))
      .flatMap(m => {
        val metaMap = RDBMIngestionUtils.caseClassToMap(m).mapValues(_.toString)
        val pkCols = m.primaryKeys.split(";").toSeq
        primaryKeys match {
          case Some(userPks) if userPks.sorted != pkCols.sorted =>
            Failure(IncorrectUserPKException(userPks, pkCols))
          case _ => Success(AuditTableInfo(m.tableName, pkCols, metaMap))
        }
      })
  }

  override def loadDataset(meta: Map[String, String]
                           , lastUpdated: Option[Timestamp]
                           , maxRowsPerPartition: Option[Int]): (Dataset[_], Column) = {
    val sqlServerTableMetadata: SQLServerTemporalTableMetadata = CaseClassConfigParser.fromMap[SQLServerTemporalTableMetadata](meta)
    val explicitColumnSelects = (for {
      startCol <- sqlServerTableMetadata.startColName
      endCol <- sqlServerTableMetadata.endColName
    } yield Seq(startCol, endCol).map(col => s"CAST($col AS DATETIME2(7)) AS $col"))
      .foldRight(Seq("0 as source_type"))((cols, dateCols) => cols ++ dateCols)
    val mainTable = sparkLoad(sqlServerTableMetadata.mainTableMetadata, lastUpdated, maxRowsPerPartition, explicitColumnSelects)
      .toDF
    val fullTable = sqlServerTableMetadata.historyTableMetadata.foldLeft(mainTable)((df, historyMetadata) => {
      val historyTable = sparkLoad(historyMetadata, lastUpdated, maxRowsPerPartition, Seq("1 as source_type"))
        .toDF

      df union historyTable.select(df.schema.fieldNames.map(historyTable(_)): _*)
    })
    (fullTable, resolveLastUpdatedColumn(sqlServerTableMetadata.mainTableMetadata, sparkSession))
  }

}


case class SQLServerTemporalTableMetadata(schemaName: String
                                          , tableName: String
                                          , historyTableSchema: Option[String] = None
                                          , historyTableName: Option[String] = None
                                          , startColName: Option[String] = None
                                          , endColName: Option[String] = None
                                          , primaryKeys: String) {

  def pkCols: Seq[String] = primaryKeys.split(";").toSeq

  def mainTableMetadata: TableExtractionMetadata = TableExtractionMetadata(schemaName, tableName, pkCols, startColName)

  def historyTableMetadata: Option[TableExtractionMetadata] = for {
    schema <- historyTableSchema
    table <- historyTableName
  } yield TableExtractionMetadata(schema, table, pkCols, endColName)

  def isTemporal: Boolean = historyTableMetadata.isDefined
}