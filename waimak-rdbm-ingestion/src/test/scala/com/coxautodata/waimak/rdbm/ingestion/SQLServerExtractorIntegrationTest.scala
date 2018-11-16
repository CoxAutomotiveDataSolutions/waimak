package com.coxautodata.waimak.rdbm.ingestion

import java.sql.{DriverManager, Timestamp}
import java.time.{ZoneOffset, ZonedDateTime}

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import com.coxautodata.waimak.rdbm.ingestion.RDBMIngestionActions._
import com.coxautodata.waimak.storage.AuditTableInfo
import com.coxautodata.waimak.storage.StorageActions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.max
import org.scalatest.BeforeAndAfterAll

import scala.util.{Failure, Success}

/**
  * Created by Ian Baynham on 19/04/18.
  */
class SQLServerExtractorIntegrationTest extends SparkAndTmpDirSpec with BeforeAndAfterAll {

  override val appName: String = "SQLServerConnectorIntegrationTest"

  val sqlServerConnectionDetails: SQLServerConnectionDetails = SQLServerConnectionDetails("localhost", 1401, "master", "SA", "SQLServer123!")
  val insertTimestamp: Timestamp = Timestamp.valueOf("2018-04-30 13:34:05.000000")
  val insertDateTime: ZonedDateTime = insertTimestamp.toLocalDateTime.atZone(ZoneOffset.UTC)

  override def beforeAll(): Unit = {
    setupTables()
  }

  override def afterAll(): Unit = {
    cleanupTables()
  }

  def setupTables(): Unit = {

    val testTableCreate =
      s"""CREATE TABLE testtable
        |(
        |     testtableID1 int NOT NULL
        |   , testtableID2 int NOT NULL
        |   , testtableValue varchar(50) NOT NULL
        |   , testtable_last_updated datetime2 NOT NULL
        |   PRIMARY KEY CLUSTERED(testtableID1, testtableID2)
        |)
        |;
        |
        |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (1, 1, 'V1', '$insertTimestamp');
        |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (2, 1, 'V2', '$insertTimestamp');
        |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (2, 2, 'V3', '$insertTimestamp');
        |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (4, 3, 'V4', '$insertTimestamp');
        |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (5, 3, 'V5', '$insertTimestamp');""".stripMargin

    val testTableSuffixCreate =
      s"""
         |CREATE TABLE testtable_suffix(
         |   testtableID1 int NOT NULL
         | , testtableID2 int NOT NULL
         | , testtableValue varchar(50) NOT NULL
         | , testtable_last_updated datetime2 NOT NULL
         |   PRIMARY KEY CLUSTERED(testtableID1, testtableID2)
         |);
         |        insert into testtable_suffix (testtableID1, testtableID2, testtableValue) VALUES (6, 4, 'V6', '$insertTimestamp');
       """.stripMargin

    executeSQl(Seq(testTableCreate, testTableSuffixCreate))
  }

  def cleanupTables(): Unit = {
    executeSQl(Seq(
       "drop table if exists testtable;"
         , "drop table if exists testtable_suffix"
    ))
  }

  def executeSQl(sqls: Seq[String]): Unit = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val connection = DriverManager.getConnection(sqlServerConnectionDetails.jdbcString, sqlServerConnectionDetails.user, sqlServerConnectionDetails.password)
    val statement = connection.createStatement
    sqls.foreach(statement.execute)
    statement.closeOnCompletion()
  }

  describe("getTableMetadata") {

      it("should return the metadata from the database (no user metadata provided)") {
        val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
        sqlServerExtractor.getTableMetadata("dbo", "testtable", None, None) should be(Success(
          AuditTableInfo("testtable", Array("testtableid1;testtableid2"), Map(
            "schemaName" -> "dbo"
            , "tableName" -> "testtable"
            , "primaryKeys" -> "testtableid1;testtableid2")
          )))
      }
    it("should return the metadata from the database with the provided last updated included") {
      val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerExtractor.getTableMetadata("dbo", "testtable", None, Some("testtable_last_updated")) should be(Success(
        AuditTableInfo("testtable", Array("testtableid1;testtableid2"), Map(
          "schemaName" -> "dbo"
          , "tableName" -> "testtable"
          , "primaryKeys" -> "testtableid1;testtableid2"
          , "lastUpdatedColumn" -> "testtable_last_updated"
        ))))
    }
    it("should fail if the user-provided pks differ from the ones found in the database") {
      val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerExtractor.getTableMetadata("dbo", "testtable", Some(Seq("incorrect_pk")), None) should be(Failure(
        IncorrectUserPKException(Seq("incorrect_pk"), Array("testtableid1;testtableid2"))
      ))
    }
    it("should return the metadata if the user-provided pks match the ones from the database") {
      val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerExtractor.getTableMetadata("dbo", "testtable", Some(Seq("testtableid1;testtableid2")), Some("testtable_last_updated")) should be(Success(
        AuditTableInfo("testtable", Array("testtableid1;testtableid2"), Map(
          "schemaName" -> "dbo"
          , "tableName" -> "testtable"
          , "primaryKeys" -> "testtableid1;testtableid2"
          , "lastUpdatedColumn" -> "testtable_last_updated"
        ))))
    }
    it("should fail if pks are not provided and they cannot be found in the database") {
      val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerExtractor.getTableMetadata("dbo", "tabledoesnotexist", None, Some("testtable_last_updated")) should be(Failure(
        PKsNotFoundOrProvidedException
      ))
    }

  }
  describe("extractToStorageFromRDBM") {

    it("should extract from tables with different suffixes to the same location") {
      val spark = sparkSession
      import spark.implicits._

      val targetTableName = "testtable_combined"

      val flow = Waimak.sparkFlow(sparkSession)
      val executor = Waimak.sparkExecutor()

      val tableConfig: Map[String, RDBMExtractionTableConfig] = Map(targetTableName -> RDBMExtractionTableConfig(targetTableName, lastUpdatedColumn = Some("testtable_last_updated")))

      val sqlServerExtractorNonSuffixed = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails, transformTableNameForRead = s => s.dropRight(9))

      val writeFlowNonSuffixed = flow.extractToStorageFromRDBM(sqlServerExtractorNonSuffixed
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
        , forceFullLoad = true
      )(targetTableName)

      executor.execute(writeFlowNonSuffixed)

      val sqlServerExtractorSuffixed = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails, transformTableNameForRead = s => s"${s.dropRight(9)}_suffix")

      val writeFlowSuffixed = flow.extractToStorageFromRDBM(sqlServerExtractorSuffixed
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
        , forceFullLoad = true
      )(targetTableName)

      val readFlow = flow.loadFromStorage(s"$testingBaseDir/output")(targetTableName)
      val res = executor.execute(readFlow)
      res._2.inputs.get[Dataset[_]](targetTableName).sort("testtableID1", "testtableID2")
        .as[TestTable].collect() should be(Seq(
        TestTable(1, 1, "V1")
        , TestTable(2, 1, "V2")
        , TestTable(3, 2, "V3")
        , TestTable(4, 3, "V4")
        , TestTable(5, 3, "V5")
        , TestTable(6, 4, "V6")
      ))

    }

    it("should extract from the db to the storage layer") {
      val spark = sparkSession
      import spark.implicits._
      val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
      val flow = Waimak.sparkFlow(sparkSession)
      val executor = Waimak.sparkExecutor()

      val tableConfig: Map[String, RDBMExtractionTableConfig] = Map("testtable" -> RDBMExtractionTableConfig("testtable", lastUpdatedColumn = Some("testtable_last_updated")))

      val writeFlow = flow.extractToStorageFromRDBM(sqlServerExtractor
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
      )("testtable")

      val res1 = executor.execute(writeFlow)
      res1._2.inputs.get[Dataset[_]]("testtable").sort("testtableID1","testtableID2")
        .as[TestTable].collect() should be(Seq(
        TestTable(1, 1, "V1")
        , TestTable(2, 1, "V2")
        , TestTable(3, 2, "V3")
        , TestTable(4, 3, "V4")
        , TestTable(5, 3, "V5")
      ))

      val updateTimestamp = Timestamp.valueOf(insertTimestamp.toLocalDateTime.plusSeconds(1))

      val inserts =
        s"""
           |insert into testtable (testtableID1, testtableID2, testtable_value) VALUES (7, 5, 'V7', '$updateTimestamp');
           |insert into testtable (testtableID1, testtableID2, testtable_value) VALUES (8, 6, 'V8', '$updateTimestamp');
           |""".stripMargin

      val updates =
        s"""
           |update testtable set (testtable_value, testtable_last_updated) = ('New Value 5', '$updateTimestamp') where testtableID1 = 5;
           |update testtable set (testtable_value, testtable_last_updated) = ('New Value 6', '$updateTimestamp') where testtableID1 = 6;
           |update testtable set (testtable_value, testtable_last_updated) = ('New Value 7', '$updateTimestamp') where testtableID1 = 7;
       """.stripMargin

      executeSQl(Seq(updates, inserts))

      val deltaWriteFlow = flow.extractToStorageFromRDBM(sqlServerExtractor
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime)("testtable")

      val res2 = executor.execute(deltaWriteFlow)

      res2._2.inputs.get[Dataset[_]]("testtable").sort("testtableID1","testtableID2")
        .as[TestTable].collect() should be(Seq(
        TestTable(5, 3, "New Value 5")
        , TestTable(6, 4, "New Value 6")
        , TestTable(7, 5, "New Value 7")
        , TestTable(8, 6, "V8")
      ))

      val deltaWriteFlowWithContingency = flow.extractToStorageFromRDBM(sqlServerExtractor
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
        , 2)("testtable")

      val res3 = executor.execute(deltaWriteFlowWithContingency)

      //We go back in time 2 seconds for contingency so we get everything back
      res3._2.inputs.get[Dataset[_]]("testtable").sort("testtableID1","testtableID2")
        .as[TestTable].collect() should be(Seq(
        TestTable(1, 1, "V1")
        , TestTable(2, 1, "V2")
        , TestTable(3, 2, "V3")
        , TestTable(4, 3, "V4")
        , TestTable(5, 3, "New Value 5")
        , TestTable(6, 4, "New Value 6")
        , TestTable(7, 5, "New Value 7")
        , TestTable(8, 6, "V8")
      ))
    }
  }

}


case class TestTable(testtableid1: Int, testtableid2: Int, testtablevalue: String )
