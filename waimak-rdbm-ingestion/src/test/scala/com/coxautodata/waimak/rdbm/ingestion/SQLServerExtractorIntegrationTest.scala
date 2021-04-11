package com.coxautodata.waimak.rdbm.ingestion

import java.sql.{DriverManager, Timestamp}
import java.time.{ZoneOffset, ZonedDateTime}
import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import com.coxautodata.waimak.rdbm.ingestion.RDBMIngestionActions._
import com.coxautodata.waimak.storage.AuditTableInfo
import com.dimafeng.testcontainers.{ForAllTestContainer, MSSQLServerContainer}
import org.scalatest.BeforeAndAfterAll

import scala.util.{Failure, Success}

class SQLServerExtractorIntegrationTest extends SparkAndTmpDirSpec with ForAllTestContainer {

  override val appName: String = "SQLServerConnectorIntegrationTest"

  override val container: MSSQLServerContainer = MSSQLServerContainer()

  lazy val sqlServerConnectionDetails: SQLServerConnectionDetails =
    SQLServerConnectionDetails("localhost", container.exposedPorts.head, container.databaseName, container.username, container.password)

  val insertTimestamp: Timestamp = Timestamp.valueOf("2018-04-30 13:34:05.000000")
  val insertDateTime: ZonedDateTime = insertTimestamp.toLocalDateTime.atZone(ZoneOffset.UTC)

  override def afterStart(): Unit = {
    setupTables()
  }

  override def beforeStop(): Unit = {
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

    executeSQl(Seq(testTableCreate, testTablePkCreate))
  }

  def cleanupTables(): Unit = {
    executeSQl(Seq(
      "drop table if exists testtable;"
      , "drop table if exists testtable_pk"
    ))
  }

  def executeSQl(sqls: Seq[String]): Unit = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val connection = DriverManager.getConnection(container.jdbcUrl, sqlServerConnectionDetails.user, sqlServerConnectionDetails.password)
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
      val sqlExtractor = new SQLServerExtractor(sparkSession, sqlServerConnectionDetails)
      val flow = Waimak.sparkFlow(sparkSession)
      val executor = Waimak.sparkExecutor()

      val tableConfig: Map[String, RDBMExtractionTableConfig] = Map("testtable" -> RDBMExtractionTableConfig("testtable"))

      val writeFlow = flow.extractToStorageFromRDBM(sqlExtractor
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
      )("testtable")
    }
  }
}

case class TestTable(testtableid1: Int, testtableid2: Int, testtablevalue: String)



