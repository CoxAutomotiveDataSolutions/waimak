package com.coxautodata.waimak.rdbm.ingestion

import java.sql.Timestamp
import java.util.Properties

import com.coxautodata.waimak.dataflow.spark.SparkSpec
import com.coxautodata.waimak.storage.AuditTableInfo
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Created by Vicky Avison on 01/05/18.
  */
class TestRDBMExtractor extends SparkSpec {

  override val appName: String = "TestRDBMExtractor"

  val extractionTimestamp: Timestamp = Timestamp.valueOf("2018-05-01 09:11:12")

  describe("selectQuery") {
    it("should generate a full select query if the table does not have a last updated column") {
      val tableMetadata = TableExtractionMetadata.fromPkSeq("dbo", "table_a", Seq("table_a_pk"), None)
      TExtractor.selectQuery(tableMetadata, Some(extractionTimestamp), Seq.empty) should be(
        "(select *, CURRENT_TIMESTAMP as system_timestamp_of_extraction from [dbo].[table_a]) s"
      )
    }
    it("should generate a full select query if the last updated is not set") {
      val tableMetadata = TableExtractionMetadata.fromPkSeq("dbo", "table_a", Seq("table_a_pk"), Some("table_a_last_updated"))
      TExtractor.selectQuery(tableMetadata, None, Seq.empty) should be(
        "(select *, CURRENT_TIMESTAMP as system_timestamp_of_extraction from [dbo].[table_a]) s"
      )
    }
    it("should generate a select query using the last updated timestamp") {
      val tableMetadata = TableExtractionMetadata.fromPkSeq("dbo", "table_a", Seq("table_a_pk"), Some("table_a_last_updated"))
      TExtractor.selectQuery(tableMetadata, Some(extractionTimestamp), Seq.empty) should be(
        "(select *, CURRENT_TIMESTAMP as system_timestamp_of_extraction from [dbo].[table_a] where [table_a_last_updated] > '2018-05-01 09:11:12.0') s"
      )
    }
    it("should add the explicit select columns to to the select statement") {
      val tableMetadata = TableExtractionMetadata.fromPkSeq("dbo", "table_a", Seq("table_a_pk"), Some("table_a_last_updated"))
      TExtractor.selectQuery(tableMetadata, Some(extractionTimestamp), Seq("ValidFrom", "ValidTo")) should be(
        "(select *, ValidFrom,ValidTo,CURRENT_TIMESTAMP as system_timestamp_of_extraction from [dbo].[table_a] where [table_a_last_updated] > '2018-05-01 09:11:12.0') s"
      )
    }
  }

  describe("splitPointsQuery") {
    it("should generate a query to find the split points with the last updated set") {
      val tableMetadata = TableExtractionMetadata.fromPkSeq("dbo", "table_a", Seq("table_a_pk"), Some("table_a_last_updated"))
      TExtractor.splitPointsQuery(tableMetadata, Some(extractionTimestamp), 15) should be(
        """(
          |select split_point from (
          |select [table_a_pk] as split_point, row_number() over (order by [table_a_pk]) as _row_num
          |from [dbo].[table_a] where [table_a_last_updated] > '2018-05-01 09:11:12.0'
          |) ids where _row_num % 15 = 0) s""".stripMargin
      )
    }
    it("should generate a query to find the split points when the last updated is not set") {
      val tableMetadata = TableExtractionMetadata.fromPkSeq("dbo", "table_a", Seq("table_a_pk"), Some("table_a_last_updated"))
      TExtractor.splitPointsQuery(tableMetadata, None, 15) should be(
        """(
          |select split_point from (
          |select [table_a_pk] as split_point, row_number() over (order by [table_a_pk]) as _row_num
          |from [dbo].[table_a]
          |) ids where _row_num % 15 = 0) s""".stripMargin
      )
    }
    it("should generate a query to find the split points for composite primary keys") {
      val compositePKTableMetadata = TableExtractionMetadata.fromPkSeq("dbo", "table_a", Seq("pk1", "pk2"), Some("table_a_last_updated"))
      TExtractor.splitPointsQuery(compositePKTableMetadata, None, 15) should be(
        """(
          |select split_point from (
          |select CONCAT([pk1],'-',[pk2]) as split_point, row_number() over (order by [pk1],[pk2]) as _row_num
          |from [dbo].[table_a]
          |) ids where _row_num % 15 = 0) s""".stripMargin
      )
    }
  }

  describe("splitPointsToPredicates") {
    val tableMetadata = TableExtractionMetadata.fromPkSeq("dbo", "table_a", Seq("table_a_pk"), Some("table_a_last_updated"))
    it("should be undefined if the split points are empty") {
      TExtractor.splitPointsToPredicates(Seq.empty, tableMetadata) should be(None)
    }
    it("should return two predicates if there is one split point") {
      TExtractor.splitPointsToPredicates(Seq("1"), tableMetadata).map(_.toList) should be(Some(List(
        "[table_a_pk] < '1'"
        , "[table_a_pk] >= '1'"))
      )
    }
    it("should return three predicates if there is are two split points") {
      TExtractor.splitPointsToPredicates(Seq("1", "5"), tableMetadata).map(_.toList) should be(Some(List(
        "[table_a_pk] >= '1' and [table_a_pk] < '5'"
        , "[table_a_pk] < '1'"
        , "[table_a_pk] >= '5'"))
      )
    }

    it("should return predicates for composite primary keys") {
      val compositePKTableMetadata = TableExtractionMetadata.fromPkSeq("dbo", "table_a", Seq("pk1", "pk2"), Some("table_a_last_updated"))
      TExtractor.splitPointsToPredicates(
        Seq("1-2", "5-4"), compositePKTableMetadata).map(_.toList) should be(
        Some(List("CONCAT([pk1],'-',[pk2]) >= '1-2' and CONCAT([pk1],'-',[pk2]) < '5-4'"
          , "CONCAT([pk1],'-',[pk2]) < '1-2'"
          , "CONCAT([pk1],'-',[pk2]) >= '5-4'")
        ))
    }
  }

}

object TExtractor extends RDBMExtractor {

  override def connectionDetails: RDBMConnectionDetails = ???

  override def driverClass: String = ???

  override def sparkSession: SparkSession = ???

  override def sourceDBSystemTimestampFunction: String = "CURRENT_TIMESTAMP"

  override def extraConnectionProperties: Properties = ???

  override def escapeKeyword(identifier: String): String = s"[$identifier]"

  override protected def getTableMetadata(dbSchemaName: String, tableName: String, primaryKeys: Option[Seq[String]], lastUpdatedColumn: Option[String], retainStorageHistory: Option[String] => Boolean): Try[AuditTableInfo] = ???
}


