package com.coxautodata.waimak.rdbm.ingestion

import java.sql.{DriverManager, Timestamp}
import java.time.{Instant, ZoneOffset, ZonedDateTime}

trait SQLServerTemporalExtractorBase {
  val sqlServerConnectionDetails: SQLServerConnectionDetails = SQLServerConnectionDetails("localhost", 1401, "master", "SA", "SQLServer123!")
  val insertTimestamp: Timestamp = Timestamp.valueOf("2018-04-30 13:34:05.000000")
  val insertDateTime: ZonedDateTime = insertTimestamp.toLocalDateTime.atZone(ZoneOffset.UTC)

  def setupTables(): Unit = {
    val testTemporalTableCreate =
      """CREATE TABLE TestTemporal
        |(
        |     TestTemporalID int NOT NULL IDENTITY PRIMARY KEY
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
        |;""".stripMargin

    val testNonTemporalTableCreate =
      """CREATE TABLE TestNonTemporal
        |(
        |     TestNonTemporalID1 int NOT NULL
        |   , TestNonTemporalID2 int NOT NULL
        |   , TestNonTemporalValue varchar(50) NOT NULL
        |   PRIMARY KEY CLUSTERED(TestNonTemporalID1, TestNonTemporalID2)
        |)
        |;""".stripMargin

    executeSQl(Seq(testTemporalTableCreate, testNonTemporalTableCreate))
  }

  def addTemporalTestData(): Unit = {
    val testTemporalDataCreate =
      """
        |SET IDENTITY_INSERT TestTemporal ON
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (1, 'Value1');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (2, 'Value2');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (3, 'Value3');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (4, 'Value4');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (5, 'Value5');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (6, 'Value6');
        |        insert into TestTemporal (TestTemporalID, TestTemporalValue) VALUES (7, 'Value7');
        |        update TestTemporal set TestTemporalValue = 'New Value 1' where TestTemporalID = 1
        |        SET IDENTITY_INSERT TestTemporal OFF""".stripMargin

    executeSQl(Seq(testTemporalDataCreate))
  }

  def addNonTemporalTestData(): Unit = {
    val testNonTemporalDataCreate =
      """   insert into TestNonTemporal (TestNonTemporalID1, TestNonTemporalID2, TestNonTemporalValue) VALUES (1, 1, 'V1');
        |   insert into TestNonTemporal (TestNonTemporalID1, TestNonTemporalID2, TestNonTemporalValue) VALUES (2, 1, 'V2');
        |   insert into TestNonTemporal (TestNonTemporalID1, TestNonTemporalID2, TestNonTemporalValue) VALUES (2, 2, 'V3');
        |   insert into TestNonTemporal (TestNonTemporalID1, TestNonTemporalID2, TestNonTemporalValue) VALUES (4, 3, 'V4');
        |   insert into TestNonTemporal (TestNonTemporalID1, TestNonTemporalID2, TestNonTemporalValue) VALUES (5, 3, 'V5');""".stripMargin

    executeSQl(Seq(testNonTemporalDataCreate))
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

  def executeSQl(sql: String): Unit = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val connection = DriverManager.getConnection(sqlServerConnectionDetails.jdbcString, sqlServerConnectionDetails.user, sqlServerConnectionDetails.password)
    val statement = connection.createStatement
    statement.execute(sql)
    statement.closeOnCompletion()
  }
}

object SQLServerTemporalExtractorBase {

  sealed trait Action[+A]
  case class Add(row: TestTemporal) extends Action[Row]
  case class Delete(row: TestTemporal) extends Action[Row]
  case class Modify(row: ModifiedRow[TestTemporal]) extends Action[ModifiedRow[Row]]

  case class AppliedAction(action: Action[DBData], ts: Instant = Instant.now())

  sealed trait DBData
  sealed trait Row extends DBData
  case class ModifiedRow[+A](old: A, current: A) extends DBData
  case class TestTemporal(testtemporalid: Option[Int], testtemporalvalue: String, source_type: Int) extends Row
  case class TestNonTemporal(testnontemporalid1: Option[Int], testnontemporalid2: Option[Int], testnontemporalvalue: String) extends Row

  object TestTemporal {
    def apply(testtemporalid: Int, testtemporalvalue: String, source_type: Int): TestTemporal =
      new TestTemporal(Some(testtemporalid), testtemporalvalue, source_type)
  }

  object TestNonTemporal {
    def apply(testnontemporalid1: Int, testnontemporalid2: Int, testnontemporalvalue: String): TestNonTemporal =
      new TestNonTemporal(Some(testnontemporalid1), Some(testnontemporalid2), testnontemporalvalue)
  }
}
