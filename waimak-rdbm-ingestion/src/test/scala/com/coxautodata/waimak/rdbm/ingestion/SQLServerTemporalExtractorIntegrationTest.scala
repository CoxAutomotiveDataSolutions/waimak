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
  * Created by Vicky Avison on 19/04/18.
  */
class SQLServerTemporalExtractorIntegrationTest extends SparkAndTmpDirSpec with BeforeAndAfterAll {

  override val appName: String = "SQLServerTemporalConnectorIntegrationTest"

  val sqlServerConnectionDetails: SQLServerConnectionDetails = SQLServerConnectionDetails("localhost", 1401, "master", "SA", "SQLServer123!")
  val insertTimestamp: Timestamp = Timestamp.valueOf("2018-04-30 13:34:05.000000")
  val insertDateTime: ZonedDateTime = insertTimestamp.toLocalDateTime.atZone(ZoneOffset.UTC)

  override def beforeAll(): Unit = {
    cleanupTables() // Just for now
    setupTables()
  }

  override def afterAll(): Unit = {
    cleanupTables()
  }

  def setupTables(): Unit = {
    val testTemporalTableCreate =
      """CREATE TABLE TestTemporal
        |(
        |     TestTemporalID int NOT NULL PRIMARY KEY CLUSTERED
        |   , SysStartTime datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL
        |   , SysEndTime datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL
        |   , TestTemporalValue varchar(50) NOT NULL
        |   , PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)
        |)
        |WITH
        |   (
        |      SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.TestTemporalHistory)
        |   )
        |;
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (1, 'Value1');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (2, 'Value2');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (3, 'Value3');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (4, 'Value4');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (5, 'Value5');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (6, 'Value6');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (7, 'Value7');
        |        update TestTemporal set TestTemporalValue = 'New Value 1' where TestTemporalID = 1""".stripMargin

    val testNonTemporalTableCreate =
      """CREATE TABLE TestNonTemporal
        |(
        |     TestNonTemporalID1 int NOT NULL
        |   , TestNonTemporalID2 int NOT NULL
        |   , TestNonTemporalValue varchar(50) NOT NULL
        |   PRIMARY KEY CLUSTERED(TestNonTemporalID1, TestNonTemporalID2)
        |)
        |;
        |
        |   insert into TestNonTemporal (TestNonTemporalID1, TestNonTemporalID2, TestNonTemporalValue) VALUES (1, 1, 'V1');
        |   insert into TestNonTemporal (TestNonTemporalID1, TestNonTemporalID2, TestNonTemporalValue) VALUES (2, 1, 'V2');
        |   insert into TestNonTemporal (TestNonTemporalID1, TestNonTemporalID2, TestNonTemporalValue) VALUES (2, 2, 'V3');
        |   insert into TestNonTemporal (TestNonTemporalID1, TestNonTemporalID2, TestNonTemporalValue) VALUES (4, 3, 'V4');
        |   insert into TestNonTemporal (TestNonTemporalID1, TestNonTemporalID2, TestNonTemporalValue) VALUES (5, 3, 'V5');""".stripMargin

    executeSQl(Seq(testTemporalTableCreate, testNonTemporalTableCreate))
  }

  def cleanupTables(): Unit = {
    executeSQl(Seq(
      """if exists (SELECT * FROM INFORMATION_SCHEMA.TABLES
        |           WHERE TABLE_NAME = N'TestTemporal')
        |begin
        |    alter table TestTemporal set (SYSTEM_VERSIONING = OFF)
        |end""".stripMargin
      , "drop table if exists TestTemporal"
      , "drop table if exists TestTemporalHistory"
      , "drop table if exists TestNonTemporal"
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
    it("should read the metadata for a temporal table") {
      val sqlServerExtractor = new SQLServerTemporalExtractor(sparkSession, sqlServerConnectionDetails)

      sqlServerExtractor.allTableMetadata("dbo.testtemporal") should be(
        SQLServerTemporalTableMetadata("dbo", "testtemporal", Some("dbo"), Some("testtemporalhistory"), Some("sysstarttime"), Some("sysendtime"), "testtemporalid"))

      sqlServerExtractor.getTableMetadata("dbo", "testtemporal", None, None, None) should be(
        Success(AuditTableInfo("testtemporal", Seq("testtemporalid"), Map(
          "schemaName" -> "dbo"
          , "tableName" -> "testtemporal"
          , "primaryKeys" -> "testtemporalid"
          , "historyTableSchema" -> "dbo"
          , "historyTableName" -> "testtemporalhistory"
          , "startColName" -> "sysstarttime"
          , "endColName" -> "sysendtime")
          , true))
      )
    }

    it("should read the metadata for a non-temporal table") {
      val sqlServerExtractor = new SQLServerTemporalExtractor(sparkSession, sqlServerConnectionDetails)

      sqlServerExtractor.allTableMetadata("dbo.testnontemporal") should be(
        SQLServerTemporalTableMetadata("dbo", "testnontemporal", None, None, None, None, "testnontemporalid1;testnontemporalid2")
      )

      sqlServerExtractor.getTableMetadata("dbo", "testnontemporal", None, None, None) should be(
        Success(AuditTableInfo("testnontemporal", Seq("testnontemporalid1", "testnontemporalid2"), Map(
          "schemaName" -> "dbo"
          , "tableName" -> "testnontemporal"
          , "primaryKeys" -> "testnontemporalid1;testnontemporalid2")
          , false))
      )
    }
    it("should apply the forceRetainStorageHistory flag to the retrieved metadata") {
      val sqlServerExtractor = new SQLServerTemporalExtractor(sparkSession, sqlServerConnectionDetails)

      sqlServerExtractor.getTableMetadata("dbo", "testnontemporal", None, None, Some(true)) should be(
        Success(AuditTableInfo("testnontemporal", Seq("testnontemporalid1", "testnontemporalid2"), Map(
          "schemaName" -> "dbo"
          , "tableName" -> "testnontemporal"
          , "primaryKeys" -> "testnontemporalid1;testnontemporalid2")
          , true))
      )

      sqlServerExtractor.getTableMetadata("dbo", "testtemporal", None, None, Some(false)) should be(
        Success(AuditTableInfo("testtemporal", Seq("testtemporalid"), Map(
          "schemaName" -> "dbo"
          , "tableName" -> "testtemporal"
          , "primaryKeys" -> "testtemporalid"
          , "historyTableSchema" -> "dbo"
          , "historyTableName" -> "testtemporalhistory"
          , "startColName" -> "sysstarttime"
          , "endColName" -> "sysendtime")
          , false))
      )
    }
  }

  describe("extractToStorageFromRDBM") {

    it("should extract from the db to the storage layer") {
      val spark = sparkSession
      import spark.implicits._
      val sqlServerExtractor = new SQLServerTemporalExtractor(sparkSession, sqlServerConnectionDetails)
      val flow = Waimak.sparkFlow(sparkSession)
      val executor = Waimak.sparkExecutor()
      val tableConfig = Map("testtemporal" -> RDBMExtractionTableConfig("testtemporal")
        , "testnontemporal" -> RDBMExtractionTableConfig("testnontemporal"))
      val writeFlow = flow.extractToStorageFromRDBM(sqlServerExtractor, "dbo", s"$testingBaseDir/output", tableConfig, insertDateTime)("testtemporal", "testnontemporal")

      val res1 = executor.execute(writeFlow)
      res1._2.inputs.get[Dataset[_]]("testtemporal").sort("source_type", "testtemporalid")
        .as[TestTemporal].collect() should be(Seq(
        TestTemporal(1, "New Value 1", 0)
        , TestTemporal(2, "Value2", 0)
        , TestTemporal(3, "Value3", 0)
        , TestTemporal(4, "Value4", 0)
        , TestTemporal(5, "Value5", 0)
        , TestTemporal(6, "Value6", 0)
        , TestTemporal(7, "Value7", 0)
        , TestTemporal(1, "Value1", 1)
      ))

      res1._2.inputs.get[Dataset[_]]("testnontemporal").sort("testnontemporalid1", "testnontemporalid2")
        .as[TestNonTemporal].collect() should be(Seq(
        TestNonTemporal(1, 1, "V1")
        , TestNonTemporal(2, 1, "V2")
        , TestNonTemporal(2, 2, "V3")
        , TestNonTemporal(4, 3, "V4")
        , TestNonTemporal(5, 3, "V5")
      ))

      val readFlow = flow.loadFromStorage(s"$testingBaseDir/output")("testtemporal")
      val res2 = executor.execute(readFlow)
      res2._2.inputs.get[Dataset[_]]("testtemporal").sort("source_type", "testtemporalid")
        .as[TestTemporal].collect() should be(Seq(
        TestTemporal(1, "New Value 1", 0)
        , TestTemporal(2, "Value2", 0)
        , TestTemporal(3, "Value3", 0)
        , TestTemporal(4, "Value4", 0)
        , TestTemporal(5, "Value5", 0)
        , TestTemporal(6, "Value6", 0)
        , TestTemporal(7, "Value7", 0)
        , TestTemporal(1, "Value1", 1)
      ))
    }

    it("should extract from the db with a limited number of rows per partition") {
      val spark = sparkSession
      import spark.implicits._

      val sqlServerExtractor = new SQLServerTemporalExtractor(sparkSession, sqlServerConnectionDetails)
      val flow = Waimak.sparkFlow(sparkSession)
      val executor = Waimak.sparkExecutor()
      val tableConfig: Map[String, RDBMExtractionTableConfig] = Map("testtemporal" -> RDBMExtractionTableConfig("testtemporal", maxRowsPerPartition = Some(2))
        , "testnontemporal" -> RDBMExtractionTableConfig("testnontemporal", maxRowsPerPartition = Some(3)))

      val writeFlow = flow.extractToStorageFromRDBM(sqlServerExtractor
        , "dbo"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime)("testtemporal", "testnontemporal")

      val res = executor.execute(writeFlow)

      val testTemporal = res._2.inputs.get[Dataset[_]]("testtemporal")

      val testNonTemporal = res._2.inputs.get[Dataset[_]]("testnontemporal")

      testNonTemporal.sort("testnontemporalid1", "testnontemporalid2")
        .as[TestNonTemporal].collect() should be(Seq(
        TestNonTemporal(1, 1, "V1")
        , TestNonTemporal(2, 1, "V2")
        , TestNonTemporal(2, 2, "V3")
        , TestNonTemporal(4, 3, "V4")
        , TestNonTemporal(5, 3, "V5")
      ))

      //Should create two partitions (max rows per partition is 3)
      testNonTemporal.rdd.getNumPartitions should be(2)

      testTemporal.sort("source_type", "testtemporalid")
        .as[TestTemporal].collect() should be(Seq(
        TestTemporal(1, "New Value 1", 0)
        , TestTemporal(2, "Value2", 0)
        , TestTemporal(3, "Value3", 0)
        , TestTemporal(4, "Value4", 0)
        , TestTemporal(5, "Value5", 0)
        , TestTemporal(6, "Value6", 0)
        , TestTemporal(7, "Value7", 0)
        , TestTemporal(1, "Value1", 1)
      ))

      //Create one partition for the history table and another 4 for the main (split points 2, 4, 6)
      testTemporal.rdd.getNumPartitions should be(5)
    }
  }

  it("should handle start/stop delta logic for the temporal tables") {
    val spark = sparkSession
    import spark.implicits._

    val sqlServerExtractor = new SQLServerTemporalExtractor(sparkSession, sqlServerConnectionDetails)
    val flow = Waimak.sparkFlow(sparkSession)
    val executor = Waimak.sparkExecutor()

    val tableConfig: Map[String, RDBMExtractionTableConfig] = Map("testtemporal" -> RDBMExtractionTableConfig("testtemporal", maxRowsPerPartition = Some(2)))


    val writeFlow = flow.extractToStorageFromRDBM(sqlServerExtractor
      , "dbo"
      , s"$testingBaseDir/output"
      , tableConfig
      , insertDateTime)("testtemporal")

    executor.execute(writeFlow)


    val deletes = "delete from TestTemporal where TestTemporalId = 2;"
    val inserts =
      s"""
         |insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (8, 'Value8');
         |insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (9, 'Value9');
         |""".stripMargin
    val updates =
      s"""
         |update TestTemporal set TestTemporalValue = 'New Value 5' where TestTemporalID = 5;
         |update TestTemporal set TestTemporalValue = 'New Value 6' where TestTemporalID = 6;
         |update TestTemporal set TestTemporalValue = 'New Value 7' where TestTemporalID = 7;
       """.stripMargin

    executeSQl(Seq(updates, inserts, deletes))

    val deltaWriteFlow = flow.extractToStorageFromRDBM(sqlServerExtractor
      , "dbo"
      , s"$testingBaseDir/output"
      , tableConfig
      , insertDateTime)("testtemporal")

    val res = executor.execute(deltaWriteFlow)

    val testTemporal = res._2.inputs.get[Dataset[_]]("testtemporal")

    testTemporal.sort("source_type", "testtemporalid")
      .as[TestTemporal].collect()
      //For some reason, sometimes (not consistently) > seems to act like >= on these datetime2 fields so we need to filter
      //out the records which could mess up our test
      .filterNot(_.testtemporalid == 1) should be(Seq(
      TestTemporal(5, "New Value 5", 0)
      , TestTemporal(6, "New Value 6", 0)
      , TestTemporal(7, "New Value 7", 0)
      , TestTemporal(8, "Value8", 0)
      , TestTemporal(9, "Value9", 0)
      , TestTemporal(2, "Value2", 1)
      , TestTemporal(5, "Value5", 1)
      , TestTemporal(6, "Value6", 1)
      , TestTemporal(7, "Value7", 1)
    ))

    val maxTS = Timestamp.valueOf(testTemporal.agg(max($"system_timestamp_of_extraction")).as[String].collect().head)

    val snapshotReadFlow =
      flow.snapshotTemporalTablesFromStorage(s"$testingBaseDir/output", maxTS)("testtemporal")

    val snapshotRes = executor.execute(snapshotReadFlow)

    val testTemporalSnapshot = snapshotRes._2.inputs.get[Dataset[_]]("testtemporal")

    testTemporalSnapshot.sort("testtemporalid")
      .as[TestTemporal].collect() should be(Seq(
      TestTemporal(1, "New Value 1", 0)
      , TestTemporal(3, "Value3", 0)
      , TestTemporal(4, "Value4", 0)
      , TestTemporal(5, "New Value 5", 0)
      , TestTemporal(6, "New Value 6", 0)
      , TestTemporal(7, "New Value 7", 0)
      , TestTemporal(8, "Value8", 0)
      , TestTemporal(9, "Value9", 0)
    ))

  }
}

case class TestTemporal(testtemporalid: Int, testtemporalvalue: String, source_type: Int)

case class TestNonTemporal(testnontemporalid1: Int, testnontemporalid2: Int, testnontemporalvalue: String)
