package com.coxautodata.waimak.rdbm.ingestion

import java.sql.{DriverManager, Timestamp}
import java.time.{ZoneOffset, ZonedDateTime}

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import com.coxautodata.waimak.rdbm.ingestion.RDBMIngestionActions._
import com.coxautodata.waimak.storage.AuditTableInfo
import com.coxautodata.waimak.storage.StorageActions._
import org.scalatest.BeforeAndAfterAll

import scala.util.{Failure, Success}

/**
  * Created by Vicky Avison on 27/04/18.
  */
class PostgresExtractorIntegrationTest extends SparkAndTmpDirSpec with BeforeAndAfterAll {

  override val appName: String = "PostgresExtractorIntegrationTest"

  val postgresConnectionDetails = PostgresConnectionDetails("localhost", 5433, "postgres", "postgres", "Postgres123!", None)
  val insertTimestamp: Timestamp = Timestamp.valueOf("2018-04-30 13:34:05.000000")
  val insertDateTime: ZonedDateTime = insertTimestamp.toLocalDateTime.atZone(ZoneOffset.UTC)

  override def beforeAll(): Unit = {
    setupTables()
  }

  override def afterAll(): Unit = {
    cleanupTables()
  }

  def setupTables(): Unit = {
    val tableACreate =
      s"""
         |CREATE TABLE table_a(
         |   table_a_pk int primary key not null,
         |   table_a_value varchar,
         |   table_a_last_updated timestamp not null
         |);
         |        insert into table_a (table_a_pk, table_a_value, table_a_last_updated) VALUES (1, 'Value1', '$insertTimestamp');
         |        insert into table_a (table_a_pk, table_a_value, table_a_last_updated) VALUES (2, 'Value2', '$insertTimestamp');
         |        insert into table_a (table_a_pk, table_a_value, table_a_last_updated) VALUES (3, 'Value3', '$insertTimestamp');
         |        insert into table_a (table_a_pk, table_a_value, table_a_last_updated) VALUES (4, 'Value4', '$insertTimestamp');
         |        insert into table_a (table_a_pk, table_a_value, table_a_last_updated) VALUES (5, 'Value5', '$insertTimestamp');
         |        insert into table_a (table_a_pk, table_a_value, table_a_last_updated) VALUES (6, 'Value6', '$insertTimestamp');
         |        insert into table_a (table_a_pk, table_a_value, table_a_last_updated) VALUES (7, 'Value7', '$insertTimestamp');
       """.stripMargin

    val tableASuffixCreate =
      s"""
         |CREATE TABLE table_a_suffix(
         |   table_a_pk int primary key not null,
         |   table_a_value varchar,
         |   table_a_last_updated timestamp not null
         |);
         |        insert into table_a_suffix (table_a_pk, table_a_value, table_a_last_updated) VALUES (8, 'Value8', '$insertTimestamp');
       """.stripMargin

    executeSQl(Seq(tableACreate, tableASuffixCreate))
  }


  def cleanupTables(): Unit = {
    executeSQl(Seq(
      "drop table if exists table_a;"
      , "drop table if exists table_a_suffix;"
    ))
  }


  def executeSQl(sqls: Seq[String]): Unit = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val connection = DriverManager.getConnection(postgresConnectionDetails.jdbcString, postgresConnectionDetails.user, postgresConnectionDetails.password)
    val statement = connection.createStatement
    sqls.foreach(statement.execute)
    statement.closeOnCompletion()
  }

  describe("getTableMetadata") {
    it("should return the metadata from the database (no user metadata provided)") {
      val postgresExtractor = new PostgresExtractor(sparkSession, postgresConnectionDetails)
      postgresExtractor.getTableMetadata("public", "table_a", None, None) should be(Success(
        AuditTableInfo("table_a", Seq("table_a_pk"), Map(
          "schemaName" -> "public"
          , "tableName" -> "table_a"
          , "primaryKeys" -> "table_a_pk")
        )))
    }
    it("should return the metadata from the database with the provided last updated included") {
      val postgresExtractor = new PostgresExtractor(sparkSession, postgresConnectionDetails)
      postgresExtractor.getTableMetadata("public", "table_a", None, Some("table_a_last_updated")) should be(Success(
        AuditTableInfo("table_a", Seq("table_a_pk"), Map(
          "schemaName" -> "public"
          , "tableName" -> "table_a"
          , "primaryKeys" -> "table_a_pk"
          , "lastUpdatedColumn" -> "table_a_last_updated"
        ))))
    }
    it("should fail if the user-provided pks differ from the ones found in the database") {
      val postgresExtractor = new PostgresExtractor(sparkSession, postgresConnectionDetails)
      postgresExtractor.getTableMetadata("public", "table_a", Some(Seq("incorrect_pk")), None) should be(Failure(
        IncorrectUserPKException(Seq("incorrect_pk"), Seq("table_a_pk"))
      ))
    }
    it("should return the metadata if the user-provided pks match the ones from the database") {
      val postgresExtractor = new PostgresExtractor(sparkSession, postgresConnectionDetails)
      postgresExtractor.getTableMetadata("public", "table_a", Some(Seq("table_a_pk")), Some("table_a_last_updated")) should be(Success(
        AuditTableInfo("table_a", Seq("table_a_pk"), Map(
          "schemaName" -> "public"
          , "tableName" -> "table_a"
          , "primaryKeys" -> "table_a_pk"
          , "lastUpdatedColumn" -> "table_a_last_updated"
        ))))
    }
    it("should fail if pks are not provided and they cannot be found in the database") {
      val postgresExtractor = new PostgresExtractor(sparkSession, postgresConnectionDetails)
      postgresExtractor.getTableMetadata("public", "tabledoesnotexist", None, Some("table_a_last_updated")) should be(Failure(
        PKsNotFoundOrProvidedException
      ))
    }
  }

  describe("extractToStorageFromRDBM") {

    it("should extract from tables with different suffixes to the same location") {
      val spark = sparkSession
      import spark.implicits._

      val targetTableName = "table_a_combined"

      val flow = Waimak.sparkFlow(sparkSession)
      val executor = Waimak.sparkExecutor()

      val tableConfig: Map[String, RDBMExtractionTableConfig] = Map(targetTableName -> RDBMExtractionTableConfig(targetTableName, lastUpdatedColumn = Some("table_a_last_updated")))

      val postgresExtractorNonSuffixed = new PostgresExtractor(sparkSession, postgresConnectionDetails, transformTableNameForRead = s => s.dropRight(9))

      val writeFlowNonSuffixed = flow.extractToStorageFromRDBM(postgresExtractorNonSuffixed
        , "public"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
        , forceFullLoad = true
      )(targetTableName)

      executor.execute(writeFlowNonSuffixed)

      val postgresExtractorSuffixed = new PostgresExtractor(sparkSession, postgresConnectionDetails, transformTableNameForRead = s => s"${s.dropRight(9)}_suffix")

      val writeFlowSuffixed = flow.extractToStorageFromRDBM(postgresExtractorSuffixed
        , "public"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
        , forceFullLoad = true
      )(targetTableName)

      executor.execute(writeFlowSuffixed)

      val readFlow = flow.loadFromStorage(s"$testingBaseDir/output")(targetTableName)
      val res = executor.execute(readFlow)
      res._2.inputs.get(targetTableName).get.sort("table_a_pk")
        .as[TableA].collect() should be(Seq(
        TableA(1, "Value1", insertTimestamp)
        , TableA(2, "Value2", insertTimestamp)
        , TableA(3, "Value3", insertTimestamp)
        , TableA(4, "Value4", insertTimestamp)
        , TableA(5, "Value5", insertTimestamp)
        , TableA(6, "Value6", insertTimestamp)
        , TableA(7, "Value7", insertTimestamp)
        , TableA(8, "Value8", insertTimestamp)
      ))

    }

    it("should extract from the db to the storage layer") {
      val spark = sparkSession
      import spark.implicits._
      val postgresExtractor = new PostgresExtractor(sparkSession, postgresConnectionDetails)
      val flow = Waimak.sparkFlow(sparkSession)
      val executor = Waimak.sparkExecutor()

      val tableConfig: Map[String, RDBMExtractionTableConfig] = Map("table_a" -> RDBMExtractionTableConfig("table_a", lastUpdatedColumn = Some("table_a_last_updated")))

      val writeFlow = flow.extractToStorageFromRDBM(postgresExtractor
        , "public"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
      )("table_a")

      val res1 = executor.execute(writeFlow)
      res1._2.inputs.get("table_a").get.sort("table_a_pk")
        .as[TableA].collect() should be(Seq(
        TableA(1, "Value1", insertTimestamp)
        , TableA(2, "Value2", insertTimestamp)
        , TableA(3, "Value3", insertTimestamp)
        , TableA(4, "Value4", insertTimestamp)
        , TableA(5, "Value5", insertTimestamp)
        , TableA(6, "Value6", insertTimestamp)
        , TableA(7, "Value7", insertTimestamp)
      ))

      val updateTimestamp = Timestamp.valueOf(insertTimestamp.toLocalDateTime.plusSeconds(1))

      val inserts =
        s"""
           |insert into table_a (table_a_pk, table_a_value, table_a_last_updated) VALUES (8, 'Value8', '$updateTimestamp');
           |insert into table_a (table_a_pk, table_a_value, table_a_last_updated) VALUES (9, 'Value9', '$updateTimestamp');
           |""".stripMargin

      val updates =
        s"""
           |update table_a set (table_a_value, table_a_last_updated) = ('New Value 5', '$updateTimestamp') where table_a_pk = 5;
           |update table_a set (table_a_value, table_a_last_updated) = ('New Value 6', '$updateTimestamp') where table_a_pk = 6;
           |update table_a set (table_a_value, table_a_last_updated) = ('New Value 7', '$updateTimestamp') where table_a_pk = 7;
       """.stripMargin

      executeSQl(Seq(updates, inserts))

      val deltaWriteFlow = flow.extractToStorageFromRDBM(postgresExtractor
        , "public"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime)("table_a")

      val res2 = executor.execute(deltaWriteFlow)

      res2._2.inputs.get("table_a").get.sort("table_a_pk")
        .as[TableA].collect() should be(Seq(
        TableA(5, "New Value 5", updateTimestamp)
        , TableA(6, "New Value 6", updateTimestamp)
        , TableA(7, "New Value 7", updateTimestamp)
        , TableA(8, "Value8", updateTimestamp)
        , TableA(9, "Value9", updateTimestamp)
      ))

      val deltaWriteFlowWithContingency = flow.extractToStorageFromRDBM(postgresExtractor
        , "public"
        , s"$testingBaseDir/output"
        , tableConfig
        , insertDateTime
        , 2)("table_a")

      val res3 = executor.execute(deltaWriteFlowWithContingency)

      //We go back in time 2 seconds for contingency so we get everything back
      res3._2.inputs.get("table_a").get.sort("table_a_pk")
        .as[TableA].collect() should be(Seq(
        TableA(1, "Value1", insertTimestamp)
        , TableA(2, "Value2", insertTimestamp)
        , TableA(3, "Value3", insertTimestamp)
        , TableA(4, "Value4", insertTimestamp)
        , TableA(5, "New Value 5", updateTimestamp)
        , TableA(6, "New Value 6", updateTimestamp)
        , TableA(7, "New Value 7", updateTimestamp)
        , TableA(8, "Value8", updateTimestamp)
        , TableA(9, "Value9", updateTimestamp)
      ))
    }
  }

}

case class TableA(table_a_pk: Int, table_a_value: String, table_a_last_updated: Timestamp)
