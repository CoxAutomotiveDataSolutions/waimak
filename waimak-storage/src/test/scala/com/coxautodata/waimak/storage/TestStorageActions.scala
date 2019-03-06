package com.coxautodata.waimak.storage

import java.sql.Timestamp
import java.time.{ZoneId, ZoneOffset}

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import com.coxautodata.waimak.storage.AuditTableFile.lowTimestamp
import com.coxautodata.waimak.storage.StorageActions._
import org.apache.spark.sql.Dataset

/**
  * Created by Vicky Avison on 11/05/18.
  */
class TestStorageActions extends SparkAndTmpDirSpec {
  override val appName: String = "TestStorageActions"

  describe("storage actions") {
    val ts1 = Timestamp.valueOf("2018-05-08 10:55:12")
    val zdt1 = ts1.toLocalDateTime.atZone(ZoneOffset.UTC)
    val ts2 = Timestamp.valueOf("2018-05-09 11:32:10")
    val zdt2 = ts2.toLocalDateTime.atZone(ZoneOffset.UTC)
    val ts3 = Timestamp.valueOf("2018-05-09 12:22:01")
    val zdt3 = ts3.toLocalDateTime.atZone(ZoneOffset.UTC)

    it("should write, read everything and read snapshots") {
      val spark = sparkSession
      import spark.implicits._
      val records = Seq(
        TRecord(1, "a", ts1)
        , TRecord(2, "b", ts2)
        , TRecord(1, "c", ts3)
      )

      val executor = Waimak.sparkExecutor()

      val writeFlow = Waimak.sparkFlow(spark)
        .addInput("t_record", Some(records.toDS()))
        .getOrCreateAuditTable(testingBaseDirName, Some(_ => AuditTableInfo("t_record", Seq("id"), Map.empty)))("t_record")
        .writeToStorage("t_record", "lastUpdated", zdt1)

      executor.execute(writeFlow)

      val readEverythingFlow = Waimak.sparkFlow(spark)
        .loadFromStorage(testingBaseDirName)("t_record")
        .loadFromStorage(testingBaseDirName, outputPrefix = Some("prefix"))("t_record")

      val res1 = executor.execute(readEverythingFlow)
      res1._2.inputs.get[Dataset[_]]("t_record").sort("lastUpdated").as[TRecord].collect() should be(records)
      res1._2.inputs.get[Dataset[_]]("prefix_t_record").sort("lastUpdated").as[TRecord].collect() should be(records)

      val snapshotFlow = Waimak.sparkFlow(spark)
        .snapshotFromStorage(testingBaseDirName, ts3)("t_record")
        .snapshotFromStorage(testingBaseDirName, ts3, outputPrefix = Some("prefix"))("t_record")

      val res2 = executor.execute(snapshotFlow)
      res2._2.inputs.get[Dataset[_]]("t_record").sort("id").as[TRecord].collect() should be(Seq(
        TRecord(1, "c", ts3)
        , TRecord(2, "b", ts2)
      ))

      res2._2.inputs.get[Dataset[_]]("prefix_t_record").sort("id").as[TRecord].collect() should be(Seq(
        TRecord(1, "c", ts3)
        , TRecord(2, "b", ts2)
      ))

    }

    it("should write with compaction in window, and read everything") {
      val spark = sparkSession
      import spark.implicits._
      val records = Seq(
        TRecord(1, "a", ts1)
        , TRecord(2, "b", ts2)
        , TRecord(1, "c", ts3)
      )

      val executor = Waimak.sparkExecutor()

      val laZts = zdt1.withZoneSameLocal(ZoneId.of("America/Los_Angeles"))

      val writeFlow = Waimak.sparkFlow(spark)
        .addInput("t_record", Some(records.toDS()))
        .getOrCreateAuditTable(testingBaseDirName, Some(_ => AuditTableInfo("t_record", Seq("id"), Map.empty)))("t_record")
        .writeToStorage("t_record", "lastUpdated", laZts, runSingleCompactionDuringWindow(10, 11))

      executor.execute(writeFlow)

      val readEverythingFlow = Waimak.sparkFlow(spark)
        .loadFromStorage(testingBaseDirName)("t_record")

      val res1 = executor.execute(readEverythingFlow)
      res1._2.inputs.get[Dataset[_]]("t_record").sort("lastUpdated").as[TRecord].collect() should be(records)

      // Should be a single cold region
      val auditTable = res1._2.inputs.getAllOfType[AuditTableFile].head
      val regions = AuditTableFile.inferRegionsWithStats(spark, auditTable.storageOps, auditTable.baseFolder, Seq("t_record"))
      regions.map(_.store_type) should be(Seq("cold"))

      regions.map(_.created_on) should be(Seq(Timestamp.valueOf("2018-05-08 17:55:12")))
    }

    it("should write with force recompaction, and read everything") {
      val spark = sparkSession
      spark.conf.set(RECOMPACT_ALL, true)
      import spark.implicits._
      val records = Seq(
        TRecord(1, "a", ts1)
        , TRecord(2, "b", ts2)
        , TRecord(1, "c", ts3)
      )

      val executor = Waimak.sparkExecutor()

      val auditTable = Storage.createFileTable(spark, new Path(testingBaseDirName), AuditTableInfo("t_record", Seq("id"), Map.empty))
        .get.asInstanceOf[AuditTableFile]

      val laZts = zdt1.withZoneSameLocal(ZoneId.of("America/Los_Angeles"))

      val writeFlow = Waimak.sparkFlow(spark)
        .addInput("t_record", Some(records.toDS()))
        .writeToStorage("t_record", auditTable, "lastUpdated", laZts)

      executor.execute(writeFlow)

      val readEverythingFlow = Waimak.sparkFlow(spark)
        .loadFromStorage(testingBaseDirName)("t_record")

      val res1 = executor.execute(readEverythingFlow)
      res1._2.inputs.get[Dataset[_]]("t_record").sort("lastUpdated").as[TRecord].collect() should be(records)

      // Should be a single cold region
      val regions = AuditTableFile.inferRegionsWithStats(spark, auditTable.storageOps, auditTable.baseFolder, Seq("t_record"))
      regions.map(_.store_type) should be(Seq("cold"))

      regions.map(_.created_on) should be(Seq(Timestamp.valueOf("2018-05-08 17:55:12")))
    }

  }

  describe("runSingleCompactionDuringWindow") {
    it("should not compact if the timestamp is before the window start") {
      val zts = Timestamp.valueOf("2018-05-09 12:00:00").toLocalDateTime.atZone(ZoneOffset.UTC)
      val regions = Seq(
        AuditTableRegionInfo("person", "hot", "0", lowTimestamp, false, 0, lowTimestamp)
      )
      // Window later in the day
      runSingleCompactionDuringWindow(13, 17)(regions, 0, zts) should be(false)
      // Window that spans two days
      runSingleCompactionDuringWindow(13, 11)(regions, 0, zts) should be(false)
    }

    it("should compact if the timestamp is in the window start") {
      val zts = Timestamp.valueOf("2018-05-09 12:00:00").toLocalDateTime.atZone(ZoneOffset.UTC)
      val regions = Seq(
        AuditTableRegionInfo("person", "hot", "0", lowTimestamp, false, 0, lowTimestamp)
      )
      // Window later in the day
      runSingleCompactionDuringWindow(11, 17)(regions, 0, zts) should be(true)
      // Window that spans two days
      runSingleCompactionDuringWindow(11, 3)(regions, 0, zts) should be(true)
    }

    it("should not compact if no hot regions exists") {
      val zts = Timestamp.valueOf("2018-05-09 12:00:00").toLocalDateTime.atZone(ZoneOffset.UTC)
      val regions = Seq(
        AuditTableRegionInfo("person", "cold", "0", lowTimestamp, false, 0, lowTimestamp)
      )
      // Valid time window
      runSingleCompactionDuringWindow(11, 17)(regions, 0, zts) should be(false)
    }

    it("should not compact if there is a cold compaction within the window") {
      val zts = Timestamp.valueOf("2018-05-09 12:00:00").toLocalDateTime.atZone(ZoneOffset.UTC)
      val tsCompact = Timestamp.valueOf("2018-05-09 11:30:00")
      val regions = Seq(
        AuditTableRegionInfo("person", "hot", "0", lowTimestamp, false, 0, lowTimestamp),
        AuditTableRegionInfo("person", "cold", "0", tsCompact, false, 0, lowTimestamp)
      )
      // Valid time window
      runSingleCompactionDuringWindow(11, 17)(regions, 0, zts) should be(false)
    }

    it("should compact if there is a cold compaction but it is older than the window") {
      val zts = Timestamp.valueOf("2018-05-09 12:00:00").toLocalDateTime.atZone(ZoneOffset.UTC)
      val tsCompact = Timestamp.valueOf("2018-05-09 10:00:00")
      val regions = Seq(
        AuditTableRegionInfo("person", "hot", "0", lowTimestamp, false, 0, lowTimestamp),
        AuditTableRegionInfo("person", "cold", "0", tsCompact, false, 0, lowTimestamp)
      )
      // Valid time window
      runSingleCompactionDuringWindow(11, 17)(regions, 0, zts) should be(true)
    }

    it("should work across timezones") {
      val zts = Timestamp.valueOf("2018-05-09 12:30:00").toLocalDateTime.atZone(ZoneId.of("America/Los_Angeles"))

      // Previous compaction before window
      val tsCompactBefore = Timestamp.valueOf("2018-05-09 18:20:00")
      val regions = Seq(
        AuditTableRegionInfo("person", "hot", "0", lowTimestamp, false, 0, lowTimestamp),
        AuditTableRegionInfo("person", "cold", "0", tsCompactBefore, false, 0, lowTimestamp)
      )
      runSingleCompactionDuringWindow(12, 13)(regions, 0, zts) should be(true)

      // Previous compaction inside window
      val tsCompactInside = Timestamp.valueOf("2018-05-09 19:20:00")
      val regionsWithCold = Seq(
        AuditTableRegionInfo("person", "hot", "0", lowTimestamp, false, 0, lowTimestamp),
        AuditTableRegionInfo("person", "cold", "0", tsCompactInside, false, 0, lowTimestamp)
      )
      runSingleCompactionDuringWindow(12, 13)(regionsWithCold, 0, zts) should be(false)
    }
  }
}

case class TRecord(id: Int, value: String, lastUpdated: Timestamp)
