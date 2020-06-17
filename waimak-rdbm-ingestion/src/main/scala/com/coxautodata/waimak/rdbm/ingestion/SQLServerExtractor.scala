package com.coxautodata.waimak.rdbm.ingestion

import java.util.Properties

import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.storage.AuditTableInfo
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
  * A mechanism for generating Waimak actions to extract data from a SQL Server instance
  * where the Database Versioning doesn't allow for STRING_AGG Functionality to be used.
  * Primarily this relates to pre SQL 2016 Databases or where STRING_AGG functionality is switched off.
  * *
  * /
  *
  * @param transformTableNameForRead How to transform the target table name into the table name in the database if the two are different.
  *                                  Useful if you have multiple tables representing the same thing but with different names, and you wish them to
  *                                  be written to the same target table
  */

class SQLServerExtractor(override val sparkSession: SparkSession
                         , sqlServerConnectionDetails: SQLServerConnectionDetails
                         , extraConnectionProperties: Properties = new Properties()
                         , override val transformTableNameForRead: String => String = identity) extends SQLServerBaseExtractor(sqlServerConnectionDetails, extraConnectionProperties) with Logging {


  val pkQuery: String =
    s"""(
       |SELECT p1.schemaName, p1.tableName,
       |stuff( (SELECT ';'+primaryKeys
       |	FROM (
       |		SELECT SCHEMA_NAME(main.schema_id) as schemaName,
       |		main.name AS tableName,
       |		tc.name as primarykeys
       |		FROM sys.tables main
       |		inner join sys.indexes i on main.object_id = i.object_id
       |		inner join sys.index_columns ic on i.object_id = ic.object_id and i.index_id = ic.index_id
       |		inner join sys.columns tc on ic.object_id = tc.object_id and ic.column_id = tc.column_id
       |		where i.is_primary_key = 1
       |		group by main.schema_id,
       |		main.name,
       |		tc.name) p2
       |	WHERE p2.tablename = p1.tablename
       |	ORDER BY primaryKeys
       | FOR XML PATH(''), TYPE).value('.', 'varchar(max)') ,1,1,'') AS primaryKeys
       |FROM (
       |	SELECT SCHEMA_NAME(main.schema_id) as schemaName,
       |	main.name AS tableName,
       |	tc.name as primarykeys
       |	FROM sys.tables main
       |	inner join sys.indexes i on main.object_id = i.object_id
       |	inner join sys.index_columns ic on i.object_id = ic.object_id and i.index_id = ic.index_id
       |	inner join sys.columns tc on ic.object_id = tc.object_id and ic.column_id = tc.column_id
       |	where i.is_primary_key = 1
       |	group by main.schema_id,
       |	main.name,
       |	tc.name) p1
       |GROUP BY schemaName,
       |tableName) m""".stripMargin

  lazy val allTablePKs: Map[String, String] = {
    import sparkSession.implicits._
    RDBMIngestionUtils.lowerCaseAll(
      sparkSession.read
        .option("driver", driverClass)
        .jdbc(connectionDetails.jdbcString, pkQuery, connectionProperties)
    )
      .as[SQLServerTableMetadata]
      .collect()
      .map(metadata => s"${metadata.schemaName}.${metadata.tableName}" -> metadata.primaryKeys).toMap
  }


  override def getTableMetadata(dbSchemaName: String
                                , tableName: String
                                , primaryKeys: Option[Seq[String]]
                                , lastUpdatedColumn: Option[String]
                                , retainStorageHistory: Option[String] => Boolean): Try[AuditTableInfo] = {
    ((primaryKeys, getTablePKs(dbSchemaName, transformTableNameForRead(tableName))) match {
      case (Some(userPKs), Some(pksFromDB)) if userPKs.sorted != pksFromDB.sorted =>
        Failure(IncorrectUserPKException(userPKs, pksFromDB))
      case (Some(userPKs), None) => Success(TableExtractionMetadata.fromPkSeq(dbSchemaName, tableName, userPKs, lastUpdatedColumn))
      case (_, Some(pksFromDB)) => Success(TableExtractionMetadata.fromPkSeq(dbSchemaName, tableName, pksFromDB, lastUpdatedColumn))
      case _ => Failure(PKsNotFoundOrProvidedException)
    }).map(meta => AuditTableInfo(meta.tableName
      , meta.pkCols
      , RDBMIngestionUtils.caseClassToMap(meta).mapValues(_.toString)
      , retainStorageHistory(meta.lastUpdatedColumn)
    ))
  }

  def getTablePKs(dbSchemaName: String
                  , tableName: String): Option[Seq[String]] = {
    allTablePKs.get(s"$dbSchemaName.$tableName").map(_.split(";").toSeq)
  }

}

case class SQLServerTableMetadata(schemaName: String
                                  , tableName: String
                                  , primaryKeys: String)
