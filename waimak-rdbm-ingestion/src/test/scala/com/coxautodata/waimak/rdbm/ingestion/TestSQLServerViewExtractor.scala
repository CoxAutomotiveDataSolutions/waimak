package com.coxautodata.waimak.rdbm.ingestion

import com.coxautodata.waimak.dataflow.spark.SparkSpec
import com.coxautodata.waimak.storage.AuditTableInfo

import scala.util.{Failure, Success}

class TestSQLServerViewExtractor extends SparkSpec {

  override val appName: String = "TestSQLServerViewExtractor"

  val sqlServerConnectionDetails: SQLServerConnectionDetails = SQLServerConnectionDetails("localhost", 1401, "master", "SA", "SQLServer123!")

  describe("getTableMetadata") {
    it("should use the provided values for the primary keys and lastUpdated") {
      val sqlServerViewExtractor = new SQLServerViewExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerViewExtractor.getTableMetadata("testSchema"
        , "testTable"
        , Some(Seq("key1", "key2"))
        , Some("lastUpdated")
        , None) should be(Success(AuditTableInfo("testTable", Seq("key1", "key2"), Map(
        "schemaName" -> "testSchema"
        , "tableName" -> "testTable"
        , "primaryKeys" -> "key1,key2"
        , "lastUpdatedColumn" -> "lastUpdated")
        , true
      )))
    }

    it("should apply the forceRetainStorageHistory flag to the retrieved metadata") {
      val sqlServerViewExtractor = new SQLServerViewExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerViewExtractor.getTableMetadata("testSchema"
        , "testTable"
        , Some(Seq("key1", "key2"))
        , Some("lastUpdated")
        , Some(false)) should be(Success(AuditTableInfo("testTable", Seq("key1", "key2"), Map(
        "schemaName" -> "testSchema"
        , "tableName" -> "testTable"
        , "primaryKeys" -> "key1,key2"
        , "lastUpdatedColumn" -> "lastUpdated")
        , false
      )))

      sqlServerViewExtractor.getTableMetadata("testSchema"
        , "testTable"
        , Some(Seq("key1", "key2"))
        , None
        , Some(true)) should be(Success(AuditTableInfo("testTable", Seq("key1", "key2"), Map(
        "schemaName" -> "testSchema"
        , "tableName" -> "testTable"
        , "primaryKeys" -> "key1,key2")
        , true
      )))
    }

    it("should fail if no pks are provided") {
      val sqlServerViewExtractor = new SQLServerViewExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerViewExtractor.getTableMetadata("testSchema"
        , "testTable"
        , None
        , Some("lastUpdated")
        , None) should be(Failure(PKsNotFoundOrProvidedException))
    }
  }
}
