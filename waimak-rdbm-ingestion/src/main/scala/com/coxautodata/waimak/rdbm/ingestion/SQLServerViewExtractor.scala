package com.coxautodata.waimak.rdbm.ingestion

import java.util.Properties

import com.coxautodata.waimak.storage.AuditTableInfo
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
  * Created by Vicky Avison on 12/04/18.
  *
  * @param transformTableNameForRead How to transform the target table name into the table name in the database if the two are different.
  *                                  Useful if you have multiple tables representing the same thing but with different names, and you wish them to
  *                                  be written to the same target table
  */
class SQLServerViewExtractor(override val sparkSession: SparkSession
                             , sqlServerConnectionDetails: SQLServerConnectionDetails
                             , extraConnectionProperties: Properties = new Properties()
                             , override val transformTableNameForRead: String => String = identity) extends SQLServerBaseExtractor(sqlServerConnectionDetails, extraConnectionProperties) {

  override def getTableMetadata(dbSchemaName: String
                                , tableName: String
                                , primaryKeys: Option[Seq[String]]
                                , lastUpdatedColumn: Option[String]
                                , retainStorageHistory: Option[String] => Boolean): Try[AuditTableInfo] = {
    (primaryKeys match {
      case Some(userPKS) => Success(TableExtractionMetadata.fromPkSeq(dbSchemaName, tableName, userPKS, lastUpdatedColumn))
      case _ => Failure(PKsNotFoundOrProvidedException)
    }).map(meta => {
      AuditTableInfo(meta.tableName, meta.pkCols, RDBMIngestionUtils.caseClassToMap(meta).mapValues(_.toString), retainStorageHistory(meta.lastUpdatedColumn))
    })
  }
}
