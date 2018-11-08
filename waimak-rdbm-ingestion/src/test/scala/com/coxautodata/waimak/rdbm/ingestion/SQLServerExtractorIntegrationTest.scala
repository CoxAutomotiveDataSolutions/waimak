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

import scala.util.Success

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
      """CREATE TABLE testtable
        |(
        |     testtableID1 int NOT NULL
        |   , testtableID2 int NOT NULL
        |   , testtableValue varchar(50) NOT NULL
        |   PRIMARY KEY CLUSTERED(testtableID1, testtableID2)
        |)
        |;
        |
        |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (1, 1, 'V1');
        |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (2, 1, 'V2');
        |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (2, 2, 'V3');
        |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (4, 3, 'V4');
        |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (5, 3, 'V5');""".stripMargin

    executeSQl(Seq(testTableCreate))
  }

  def cleanupTables(): Unit = {
    executeSQl(Seq(
       "drop table if exists testtable"
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
          AuditTableInfo("testtable", Seq("testtableid1", "testtableid2"), Map(
            "schemaName" -> "dbo"
            , "tableName" -> "testtable"
            , "primaryKeys" -> "testtableid1;testtableid2")
          )))
      }

  }

    describe("extractToStorageFromRDBM") {

      it("should extract from the db to the storage layer") {
        val spark = sparkSession
        import spark.implicits._
        val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
        val flow = Waimak.sparkFlow(sparkSession)
        val executor = Waimak.sparkExecutor()
        val tableConfig = Map("testtable" -> RDBMExtractionTableConfig("testtable"))
        val writeFlow = flow.extractToStorageFromRDBM(sqlServerExtractor, "dbo", s"$testingBaseDir/output", tableConfig, insertDateTime)("testtable")

        val res1 = executor.execute(writeFlow)
        res1._2.inputs.get[Dataset[_]]("testtable").sort("testtableid1", "testtableid2")
          .as[TestTable].collect() should be(Seq(
          TestTable(1, 1, "V1")
          , TestTable(2, 1, "V2")
          , TestTable(2, 2, "V3")
          , TestTable(4, 3, "V4")
          , TestTable(5, 3, "V5")
        ))
      }

      it("should extract from the db with a limited number of rows per partition") {
        val spark = sparkSession
        import spark.implicits._

        val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
        val flow = Waimak.sparkFlow(sparkSession)
        val executor = Waimak.sparkExecutor()
        val tableConfig: Map[String, RDBMExtractionTableConfig] = Map("testtable" -> RDBMExtractionTableConfig("testtable", maxRowsPerPartition = Some(3)))

        val writeFlow = flow.extractToStorageFromRDBM(sqlServerExtractor
          , "dbo"
          , s"$testingBaseDir/output"
          , tableConfig
          , insertDateTime)("testtable")

        val res = executor.execute(writeFlow)

        val testTable = res._2.inputs.get[Dataset[_]]("testtable")

        testTable.sort("testtableid1", "testtableid2")
          .as[TestTable].collect() should be(Seq(
          TestTable(1, 1, "V1")
          , TestTable(2, 1, "V2")
          , TestTable(2, 2, "V3")
          , TestTable(4, 3, "V4")
          , TestTable(5, 3, "V5")
        ))

        //Should create two partitions (max rows per partition is 3)
        testTable.rdd.getNumPartitions should be(2)

      }
    }

  }

case class TestTable(testtableid1: Int, testtableid2: Int, testtablevalue: String)
