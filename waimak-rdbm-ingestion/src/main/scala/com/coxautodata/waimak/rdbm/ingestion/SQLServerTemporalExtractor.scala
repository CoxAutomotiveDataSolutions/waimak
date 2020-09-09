package com.coxautodata.waimak.rdbm.ingestion

import java.sql.Timestamp
import java.util.Properties

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.log.Level._
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

  val defaultUpperTimestamp = "9999-12-31 23:59:59"

  val metadataQuery: String =
    s"""(
       | SELECT SCHEMA_NAME(main.schema_id) as schemaName,
       | main.name AS tableName,
       | SCHEMA_NAME (history.schema_id) as historyTableSchema,
       | history.name as historyTableName,
       | colstart.name as startColName,
       | colend.name as endColName,
       | STRING_AGG(tc.name, ';') as primaryKeys,
       | '$defaultUpperTimestamp' as databaseUpperTimestamp
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

  val lowerDateBound = "1970-01-01"
  val upperDateBound = "9999-12-31"

  def castToDateTime7(col: String): String =
    s"cast(${escapeKeyword(col)} as datetime2(7))"

  /** This query selects the max timestamp for a history tables endCol. This is here because for some versions of sql
   * server we saw the end timestamp being 9999-12-31 23:59:59.0000000, whereas for others the end timestamp would be
   * 9999-12-31 23:59:59.9999999. Instead of trying to hard code this, we decided that we would instead try to detect
   * which of these cases we were in, by selecting the max value from the endCol. In practice, if all the rows were deleted
   * this would be much less than 9999-12-31, however in this case there would be no value in selecting less than the max
   * anyway.
   *
   * When the table is empty the query returns null, so in this case we just use the default timestamp. */
  def upperTimestamp(tableMetadata: ExtractionMetadata): String = {
    import sparkSession.implicits._

    tableMetadata.endColName match {
      case Some(endCol) => {
        val query = s"select coalesce(cast(max(${castToDateTime7(endCol)}) as nvarchar), '$defaultUpperTimestamp') as databaseUpperTimestamp " +
          s"from ${tableMetadata.qualifiedTableName(escapeKeyword)}"
        RDBMIngestionUtils.lowerCaseAll(
          sparkSession.read.format("jdbc")
            .option("driver", driverClass)
            .option("url", connectionDetails.jdbcString)
            .option("user", connectionDetails.user)
            .option("password", connectionDetails.password)
            .option("dbtable", s"($query) as maxTs")
            .load
        )
          .as[String]
          .collect()
          .head
      }
      case None => defaultUpperTimestamp
    }
  }

  override def getTableMetadata(dbSchemaName: String
                                , tableName: String
                                , primaryKeys: Option[Seq[String]]
                                , lastUpdatedColumn: Option[String]
                                , retainStorageHistory: Option[String] => Boolean): Try[AuditTableInfo] = {
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
          case _ => Success(AuditTableInfo(m.tableName, pkCols, metaMap, retainStorageHistory(m.mainTableMetadata.lastUpdatedColumn)))
        }
      })
  }

  override def loadDataset(meta: Map[String, String]
                           , lastUpdated: Option[Timestamp]
                           , maxRowsPerPartition: Option[Int]): (Dataset[_], Column) = {
    val sqlServerTableMetadata: SQLServerTemporalTableMetadata = CaseClassConfigParser.fromMap[SQLServerTemporalTableMetadata](meta)

    val explicitColumnSelects: Seq[String] = (for {
      startCol <- sqlServerTableMetadata.startColName
      endCol <- sqlServerTableMetadata.endColName
    } yield Seq(startCol, endCol).map(col => castToDateTime7(col) + s" as $col"))
      .foldRight(Seq[String]())((cols, dateCols) => cols ++ dateCols)

    val table = sparkLoad(sqlServerTableMetadata, lastUpdated, maxRowsPerPartition, explicitColumnSelects)
    logInfo(s"Loaded sql server temporal dataset from ${sqlServerTableMetadata.tableName}")
    (table, resolveLastUpdatedColumn(sqlServerTableMetadata.mainTableMetadata, sparkSession))
  }

  override def selectQuery(tableMetadata: ExtractionMetadata, lastUpdated: Option[Timestamp], explicitColumnSelects: Seq[String]): String = {
    val upperTS = upperTimestamp(tableMetadata)
    val tableMetadataWithUpperBound = tableMetadata.setUpperTimestamp(upperTS)
    val extraSelectCols = extraSelects(tableMetadataWithUpperBound, explicitColumnSelects)

    logAndReturn(
      s"""(select * $extraSelectCols ${fromQueryPart(tableMetadataWithUpperBound, lastUpdated)}) s""",
      (query: String) => s"Query: $query for metadata ${tableMetadataWithUpperBound.toString} for lastUpdated ${lastUpdated}",
      Debug
    )
  }

  override def fromQueryPart(tableMetadata: ExtractionMetadata, lastUpdated: Option[Timestamp]): String = {
    logInfo(s"table meta: ${tableMetadata} lastUpdated: ${lastUpdated}")

    (tableMetadata.historyTableName, tableMetadata.startColName, tableMetadata.endColName, lastUpdated) match {
      case (Some(_), Some(startCol), Some(endCol), Some(ts)) =>
        logInfo("Delta flow query")
        s"""from ${tableMetadata.qualifiedTableName(escapeKeyword)}
           |for SYSTEM_TIME from '$ts' to '$upperDateBound'
           |where (${escapeKeyword(endCol)} < ${upperTS(tableMetadata)} and ${escapeKeyword(endCol)} >= '$ts')
           |or ${escapeKeyword(startCol)} >= '$ts'""".stripMargin
      // All we care about here is that we are in a history table, this is the case where we want all the history unified
      case (Some(_), Some(_), Some(_), None) =>
        logInfo("Get all history query")
        s"""from ${tableMetadata.qualifiedTableName(escapeKeyword)}
           |for SYSTEM_TIME from '$lowerDateBound' to '$upperDateBound'""".stripMargin
      // If we have no history information then do a normal select from
      case _ =>
        s"""from ${tableMetadata.qualifiedTableName(escapeKeyword)}"""
    }
  }

  private def extraSelects(tableMetadata: ExtractionMetadata, explicitColumnSelects: Seq[String]): String = {
    val fixed =
      tableMetadata.lastUpdatedColumn match {
        case Some(_) => explicitColumnSelects
        case None => explicitColumnSelects :+ s"$sourceDBSystemTimestampFunction as $systemTimestampColumnName"
      }

    val selects = tableMetadata.endColName.map(fixed :+ sourceType(_, tableMetadata)).getOrElse(fixed)
    if (selects.isEmpty) "" else ", " + selects.mkString(",")
  }

  private def sourceType(endColName: String, tableMetadata: ExtractionMetadata): String = {
    s"""source_type =
       |  case
       |    when ${escapeKeyword(endColName)} = ${upperTS(tableMetadata)} then 0
       |    else 1
       |  end
       |""".stripMargin
  }

  private def upperTS(tableMetadata: ExtractionMetadata): String =
    s"'${tableMetadata.databaseUpperTimestamp.getOrElse(defaultUpperTimestamp)}'"
}