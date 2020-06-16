package com.coxautodata.waimak.rdbm.ingestion

import java.sql.Timestamp
import java.util.Properties

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.storage.AuditTableInfo
import org.apache.spark.sql.{Column, Dataset, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  * Created by Vicky Avison on 27/04/18.
  *
  * @param transformTableNameForRead How to transform the target table name into the table name in the database if the two are different.
  *                                  Useful if you have multiple tables representing the same thing but with different names, and you wish them to
  *                                  be written to the same target table
  */
class PostgresExtractor(override val sparkSession: SparkSession
                        , override val connectionDetails: PostgresConnectionDetails
                        , override val extraConnectionProperties: Properties = new Properties()
                        , override val transformTableNameForRead: String => String = identity) extends RDBMExtractor {


  override def driverClass: String = "org.postgresql.Driver"

  override def sourceDBSystemTimestampFunction: String = "CURRENT_TIMESTAMP"

  override def escapeKeyword(identifier: String) = s""""$identifier""""


  val pkQuery: String =
    s"""
       |(
       |select
       |   n.nspname as schemaName,
       |   t.relname as tableName,
       |   STRING_AGG(a.attname, ';') as pkCols
       |from     pg_class t,
       |    pg_class i,
       |    pg_index ix,
       |    pg_namespace n,
       |    pg_attribute a
       |where
       |    t.oid = ix.indrelid
       |    and i.oid = ix.indexrelid
       |    and n.oid = t.relnamespace
       |    and ix.indisprimary
       |    and a.attrelid = t.oid
       |    and a.attnum = ANY(ix.indkey)
       |
       |group by n.nspname, t.relname
       |) s
   """.stripMargin

  lazy val allTablePKs: Map[String, String] = {
    import sparkSession.implicits._
    RDBMIngestionUtils.lowerCaseAll(
      sparkSession.read
        .option("driver", driverClass)
        .jdbc(connectionDetails.jdbcString, pkQuery, connectionProperties)
    )
      .as[PostgresMetadata]
      .collect()
      .map(metadata => s"${metadata.schemaName}.${metadata.tableName}" -> metadata.pkCols).toMap
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
    }).map(meta => AuditTableInfo(meta.tableName, meta.primaryKeysSeq, RDBMIngestionUtils.caseClassToMap(meta).mapValues(_.toString), retainStorageHistory(meta.lastUpdatedColumn)))
  }


  def getTablePKs(dbSchemaName: String
                  , tableName: String): Option[Seq[String]] = {
    allTablePKs.get(s"$dbSchemaName.$tableName").map(_.split(";").toSeq)
  }

  override def loadDataset(meta: Map[String, String], lastUpdated: Option[Timestamp], maxRowsPerPartition: Option[Int]): (Dataset[_], Column) = {
    super.loadDataset(meta, lastUpdated, maxRowsPerPartition)
  }

}

case class PostgresConnectionDetails(server: String
                                     , port: Int
                                     , databaseName: String
                                     , user: String
                                     , password: String
                                     , sslFactory: Option[String]) extends RDBMConnectionDetails {
  val jdbcString: String = sslFactory.foldLeft(s"jdbc:postgresql://$server:$port/$databaseName")((s, factory) => {
    s"$s?ssl=true&sslfactory=$factory"
  })
}


case class PostgresMetadata(schemaName: String
                            , tableName: String
                            , pkCols: String)