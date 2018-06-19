package com.coxautodata.waimak.rdbm.ingestion

import java.sql.Timestamp

import com.coxautodata.waimak.dataflow.spark.SparkSpec

/**
  * Created by Vicky Avison on 08/05/18.
  */
class TestRDBMIngestionUtils extends SparkSpec {

  override val appName: String = "TestRDBMIngestionUtils"

  describe("snapshotTemporalTableDataset") {

    val insertTimestamp = Timestamp.valueOf("2018-05-08 10:15:00")
    val updateTimestamp = Timestamp.valueOf("2018-05-08 11:20:12")
    val deleteTimestamp = Timestamp.valueOf("2018-05-08 12:10:01")
    val newInsertTimestamp = Timestamp.valueOf("2018-05-08 13:03:36")

    val endOfTime = Timestamp.valueOf("9999-12-31 11:59:59")

    val temporalTableMetadata = SQLServerTemporalTableMetadata("dbo", "testtemporal", Some("dbo"), Some("testtemporalhistory")
      , Some("validfrom"), Some("validto"), "key")

    val preSnapshotRecords = Seq(
      //Initial inserts
      TTemporalRecord(1, "a", insertTimestamp, endOfTime)
      , TTemporalRecord(2, "b", insertTimestamp, endOfTime)
      , TTemporalRecord(3, "c", insertTimestamp, endOfTime)
      , TTemporalRecord(4, "d", insertTimestamp, endOfTime)
      //Update record 1
      , TTemporalRecord(1, "e", updateTimestamp, endOfTime)
      , TTemporalRecord(1, "a", insertTimestamp, updateTimestamp)
      //Delete records 2 and 3
      , TTemporalRecord(2, "b", insertTimestamp, deleteTimestamp)
      , TTemporalRecord(3, "c", insertTimestamp, deleteTimestamp)
      //Reinsert 2 with a different value
      , TTemporalRecord(2, "f", newInsertTimestamp, endOfTime)
    )
    val timeAfterAllEvents = Timestamp.valueOf("2018-05-08 13:04:00")

    it("should return the latest records for each primary key, minus any deleted records with a snapshot timestamp" +
      "after all events have happened") {
      val spark = sparkSession
      import spark.implicits._
      RDBMIngestionUtils.snapshotTemporalTableDataset(preSnapshotRecords.toDS, timeAfterAllEvents, temporalTableMetadata)
        .sort("key").as[TTemporalRecord].collect() should be(Seq(
        TTemporalRecord(1, "e", updateTimestamp, endOfTime)
        , TTemporalRecord(2, "f", newInsertTimestamp, endOfTime)
        , TTemporalRecord(4, "d", insertTimestamp, endOfTime)))
    }

    it("should include the deleted records if the snapshot timestamp is before the time of deletion") {
      val spark = sparkSession
      import spark.implicits._
      val timeBeforeDeleteButAfterOtherEvents = Timestamp.valueOf("2018-05-08 12:09:00")
      RDBMIngestionUtils.snapshotTemporalTableDataset(preSnapshotRecords.toDS, timeBeforeDeleteButAfterOtherEvents, temporalTableMetadata)
        .sort("key").as[TTemporalRecord].collect() should be(Seq(
        TTemporalRecord(1, "e", updateTimestamp, endOfTime)
        , TTemporalRecord(2, "b", insertTimestamp, deleteTimestamp)
        , TTemporalRecord(3, "c", insertTimestamp, deleteTimestamp)
        , TTemporalRecord(4, "d", insertTimestamp, endOfTime)))

    }

    it("should return the original version of an updated record if the snapshot timestamp is before the update timestamp") {
      val spark = sparkSession
      import spark.implicits._
      RDBMIngestionUtils.snapshotTemporalTableDataset(preSnapshotRecords.toDS, insertTimestamp, temporalTableMetadata)
        .sort("key").as[TTemporalRecord].collect() should be(Seq(
        TTemporalRecord(1, "a", insertTimestamp, updateTimestamp)
        , TTemporalRecord(2, "b", insertTimestamp, deleteTimestamp)
        , TTemporalRecord(3, "c", insertTimestamp, deleteTimestamp)
        , TTemporalRecord(4, "d", insertTimestamp, endOfTime)
      ))
    }

    it("should assume the record exists if there is any ambiguity caused by event collisions") {
      //Day zero pull from history and main tables
      val preSnapshotWithEventCollisions = Seq(
        TTemporalRecord(1, "a", insertTimestamp, updateTimestamp)
        //This was updated twice within the same timestamp
        , TTemporalRecord(1, "b", updateTimestamp, updateTimestamp)
        , TTemporalRecord(1, "c", updateTimestamp, endOfTime)
      )
      val spark = sparkSession
      import spark.implicits._
      RDBMIngestionUtils.snapshotTemporalTableDataset(preSnapshotWithEventCollisions.toDS, timeAfterAllEvents, temporalTableMetadata)
        .as[TTemporalRecord].collect() should be(Seq(
        TTemporalRecord(1, "c", updateTimestamp, endOfTime)
      ))
    }
  }
}

case class TTemporalRecord(key: Int, value: String, validFrom: Timestamp, validTo: Timestamp)
