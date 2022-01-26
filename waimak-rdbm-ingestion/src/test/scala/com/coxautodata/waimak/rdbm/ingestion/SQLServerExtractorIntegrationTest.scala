package com.coxautodata.waimak.rdbm.ingestion

import java.sql.{DriverManager, Timestamp}
import java.time.{ZoneOffset, ZonedDateTime}
import com.coxautodata.waimak.dataflow.{DataFlowException, Waimak}
import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import com.coxautodata.waimak.rdbm.ingestion.RDBMIngestionActions._
import com.coxautodata.waimak.storage.{AuditTableInfo, StorageException}
import org.apache.spark.sql.Dataset
import org.scalatest.BeforeAndAfterAll

import scala.util.{Failure, Success}

class SQLServerExtractorIntegrationTest extends SparkAndTmpDirSpec with BeforeAndAfterAll {

  override val appName: String = "SQLServerConnectorIntegrationTest"

  val sqlServerConnectionDetails: SQLServerConnectionDetails = SQLServerConnectionDetails("localhost", 1401, "master", "SA", "SQLServer123!")
  val insertTimestamp: Timestamp = Timestamp.valueOf("2018-04-30 13:34:05.000000")
  val insertDateTime: ZonedDateTime = insertTimestamp.toLocalDateTime.atZone(ZoneOffset.UTC)

  val nowDatetime = "(select cast(getdate() as datetime))"

  val testEmptyTableInserts =
    s"""
       | insert into testtableemptydatetime (id1, id2, moddt, sometext) values (1, 1, $nowDatetime, 'v1');
       | insert into testtableemptydatetime (id1, id2, moddt, sometext) values (2, 3, $nowDatetime, 'v2');
       |""".stripMargin

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
         |   PRIMARY KEY CLUSTERED(testtableID1, testtableID2)
         |)
         |;
         |
         |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (1, 1, 'V1');
         |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (2, 1, 'V2');
         |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (2, 2, 'V3');
         |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (4, 3, 'V4');
         |   insert into testtable (testtableID1, testtableID2, testtableValue) VALUES (5, 3, 'V5');""".stripMargin

    val testTablePkCreate =
      s"""
         |CREATE TABLE testtable_pk(
         |   testtableID1 int NOT NULL
         | , testtableID2 int NOT NULL
         | , testtableValue varchar(50) NOT NULL
         |);
         |
         |  insert into testtable_pk (testtableID1, testtableID2, testtableValue) VALUES (6, 4, 'V6');
       """.stripMargin

    val testTableEmptyCreate =
      s"""
         |CREATE TABLE testtableempty
         |(
         |    id1 int not null,
         |    id2 int not null,
         |    moddt datetime2,
         |    sometext varchar(50)
         |)
         |""".stripMargin

    val testTableEmptyDatetimeCreate =
      s"""
         |CREATE TABLE testtableemptydatetime
         |(
         |    id1 int not null,
         |    id2 int not null,
         |    moddt datetime,
         |    sometext varchar(50)
         |)
         |""".stripMargin

    executeSQl(Seq(testTableCreate, testTablePkCreate, testTableEmptyCreate, testTableEmptyDatetimeCreate))
  }

  def cleanupTables(): Unit = {
    executeSQl(Seq(
      "drop table if exists testtable;"
      , "drop table if exists testtable_pk"
      , "drop table if exists testtableempty"
      , "drop table if exists testtableemptydatetime"
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
      sqlServerExtractor.getTableMetadata("dbo", "testtable", None, None, None) should be(Success(
        AuditTableInfo("testtable", Seq("testtableid1", "testtableid2"), Map(
          "schemaName" -> "dbo"
          , "tableName" -> "testtable"
          , "primaryKeys" -> "testtableid1,testtableid2"
        )
          , false
        )))
    }

    it("should fail if the user-provided pks differ from the ones found in the database") {
      val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerExtractor.getTableMetadata("dbo", "testtable", Some(Seq("incorrect_pk")), None, None) should be(Failure(
        IncorrectUserPKException(Seq("incorrect_pk"), Seq("testtableid1", "testtableid2"))
      ))
    }
    it("should return metadata if there are user-provided pks but table pk's not specified in the database") {
      val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerExtractor.getTableMetadata("dbo", "testtable_pk", Some(Seq("testtableid1", "testtableid2")), None, None) should be(Success(
        AuditTableInfo("testtable_pk", Seq("testtableid1", "testtableid2"), Map(
          "schemaName" -> "dbo"
          , "tableName" -> "testtable_pk"
          , "primaryKeys" -> "testtableid1,testtableid2"
        )
          , false)))
    }
    it("should fail if no user-provided pks and table pk's specified in the database") {
      val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerExtractor.getTableMetadata("dbo", "testtable_pk", None, None, None) should be(Failure(
        PKsNotFoundOrProvidedException
      ))
    }
    it("should return the metadata if the user-provided pks match the ones from the database") {
      val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerExtractor.getTableMetadata("dbo", "testtable", Some(Seq("testtableid1", "testtableid2")), None, None) should be(Success(
        AuditTableInfo("testtable", Seq("testtableid1", "testtableid2"), Map(
          "schemaName" -> "dbo"
          , "tableName" -> "testtable"
          , "primaryKeys" -> "testtableid1,testtableid2"
        )
          , false)))
    }
    it("should apply the forceRetainStorageHistory flag to the retrieved metadata") {
      val sqlServerExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
      sqlServerExtractor.getTableMetadata("dbo", "testtable", Some(Seq("testtableid1", "testtableid2")), None, Some(true)) should be(Success(
        AuditTableInfo("testtable", Seq("testtableid1", "testtableid2"), Map(
          "schemaName" -> "dbo"
          , "tableName" -> "testtable"
          , "primaryKeys" -> "testtableid1,testtableid2"
        )
          , true)))
    }
  }
  describe("extractToStorageFromRDBM") {

    it("should extract from the db to the storage layer") {
      val spark = sparkSession
      val sqlExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails, checkLastUpdatedTimestampRange = true)
      val flow = Waimak.sparkFlow(sparkSession)
      val executor = Waimak.sparkExecutor()

      val tableConfig: Map[String, RDBMExtractionTableConfig] = Map(
        "testtable" -> RDBMExtractionTableConfig("testtable"),
        "testtableempty" -> RDBMExtractionTableConfig("testtableempty", lastUpdatedColumn = Some("moddt"), pkCols = Some(Seq("id1"))),
        "testtableemptydatetime" -> RDBMExtractionTableConfig("testtableemptydatetime", lastUpdatedColumn = Some("moddt"), pkCols = Some(Seq("id1")))
      )

      val writeFlow = flow.extractToStorageFromRDBM(sqlExtractor
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
      )(tableConfig.keySet.toSeq: _*)

      executor.execute(writeFlow)

      Thread.sleep(400)

      executor.execute(writeFlow)
    }

    it("should fail to extract when the checkLastUpdatedTimestampRange flag is not set on an empty table") {
      val spark = sparkSession
      val sqlExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails, checkLastUpdatedTimestampRange = false)
      val flow = Waimak.sparkFlow(sparkSession)
      val executor = Waimak.sparkExecutor()

      val tableConfig: Map[String, RDBMExtractionTableConfig] = Map(
        "testtableemptydatetime" -> RDBMExtractionTableConfig("testtableemptydatetime", lastUpdatedColumn = Some("moddt"), pkCols = Some(Seq("id1")))
      )

      val writeFlow = flow.extractToStorageFromRDBM(sqlExtractor
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
      )(tableConfig.keySet.toSeq: _*)

      executor.execute(writeFlow)

      Thread.sleep(400)

      val ex = intercept[DataFlowException](
        executor.execute(writeFlow)
      )

      ex.cause.asInstanceOf[StorageException].text should include("Error appending data to table [testtableemptydatetime]")
    }

    it("should extract correctly after inserting rows into a table with a datetime col") {
      val spark = sparkSession
      val sqlExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails, checkLastUpdatedTimestampRange = true)
      val flow = Waimak.sparkFlow(sparkSession)
      val executor = Waimak.sparkExecutor()

      import spark.implicits._

      val tableConfig: Map[String, RDBMExtractionTableConfig] = Map(
        "testtableemptydatetime" -> RDBMExtractionTableConfig("testtableemptydatetime", lastUpdatedColumn = Some("moddt"), pkCols = Some(Seq("id1")))
      )

      val writeFlow = flow.extractToStorageFromRDBM(sqlExtractor
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
      )(tableConfig.keySet.toSeq: _*)

      executor.execute(writeFlow)

      Thread.sleep(400)

      executor.execute(writeFlow)

      // insert some rows so we can extract
      executeSQl(Seq(testEmptyTableInserts))

      Thread.sleep(400)

      val output = executor.execute(writeFlow)

      output._2.inputs.get[Dataset[_]]("testtableemptydatetime")
        .select("id1", "id2", "sometext")
        .as[TestTableDatatime].collect() should contain theSameElementsAs Seq(
        TestTableDatatime(1, 1, "v1"),
        TestTableDatatime(2, 3, "v2")
      )

      // Clear the tables for any other testing

      cleanupTables()
      setupTables()
    }
  }
}

case class TestTable(testtableid1: Int, testtableid2: Int, testtablevalue: String)

case class TestTableDatatime(id1: Int, id2: Int, sometext: String)



