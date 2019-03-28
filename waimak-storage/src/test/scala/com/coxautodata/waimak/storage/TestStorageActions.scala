package com.coxautodata.waimak.storage

import java.sql.Timestamp
import java.time.{ZoneId, ZoneOffset}

import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import com.coxautodata.waimak.dataflow.{DataFlowException, EmptyFlowContext, Waimak}
import com.coxautodata.waimak.storage.AuditTableFile.lowTimestamp
import com.coxautodata.waimak.storage.StorageActions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

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
        .getOrCreateAuditTable(testingBaseDirName, Some(_ => AuditTableInfo("t_record", Seq("id"), Map.empty, true)))("t_record")
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
        .getOrCreateAuditTable(testingBaseDirName, Some(_ => AuditTableInfo("t_record", Seq("id"), Map.empty, true)))("t_record")
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

      val laZts = zdt1.withZoneSameLocal(ZoneId.of("America/Los_Angeles"))

      val writeFlow = Waimak.sparkFlow(spark)
        .addInput("t_record", Some(records.toDS()))
        .getOrCreateAuditTable(testingBaseDirName, Some(_ => AuditTableInfo("t_record", Seq("id"), Map.empty, true)))("t_record")
        .writeToStorage("t_record", "lastUpdated", laZts)

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

  }

  describe("getOrCreateAuditTable") {
    it("should fail if a table was not found in the storage layer") {
      val spark = sparkSession

      val executor = Waimak.sparkExecutor()

      val writeFlow = Waimak.sparkFlow(spark)
        .getOrCreateAuditTable(testingBaseDirName, None)("t_record")

      intercept[DataFlowException](
        executor.execute(writeFlow)
      ).cause.getMessage should be("The following tables were not found in the storage layer and could not be created as no metadata function was defined: t_record")
    }

    it("should update table metadata if requested") {
      val spark = sparkSession

      val executor = Waimak.sparkExecutor()

      val flow1 = Waimak.sparkFlow(spark)
        .getOrCreateAuditTable(testingBaseDirName, Some(_ => AuditTableInfo("t_record", Seq("id"), Map.empty, true)))("t_record")

      val res1 = executor.execute(flow1)
      res1._2.inputs.get[AuditTableFile]("audittable_t_record").tableInfo should be(AuditTableInfo("t_record", Seq("id"), Map.empty, true))

      val flow2 = Waimak.sparkFlow(spark)
        .getOrCreateAuditTable(testingBaseDirName, Some(_ => AuditTableInfo("t_record", Seq("id1", "id2"), Map.empty, false)))("t_record")

      //ignore new metadata because flag is not set
      val res2 = executor.execute(flow2)
      res2._2.inputs.get[AuditTableFile]("audittable_t_record").tableInfo should be(AuditTableInfo("t_record", Seq("id"), Map.empty, true))

      //update metadata
      spark.conf.set(UPDATE_TABLE_METADATA, true)
      val res3 = executor.execute(flow2)
      res3._2.inputs.get[AuditTableFile]("audittable_t_record").tableInfo should be(AuditTableInfo("t_record", Seq("id1", "id2"), Map.empty, false))

      //Check new metadata has been persisted
      spark.conf.set(UPDATE_TABLE_METADATA, false)
      val res4 = executor.execute(flow1)
      res4._2.inputs.get[AuditTableFile]("audittable_t_record").tableInfo should be(AuditTableInfo("t_record", Seq("id1", "id2"), Map.empty, false))
    }


    it("should fail if metadata update is requested but not metadata retrieval function is provided") {
      val spark = sparkSession

      val executor = Waimak.sparkExecutor()

      val flow1 = Waimak.sparkFlow(spark)
        .getOrCreateAuditTable(testingBaseDirName, Some(_ => AuditTableInfo("t_record", Seq("id"), Map.empty, true)))("t_record")

      val res1 = executor.execute(flow1)
      res1._2.inputs.get[AuditTableFile]("audittable_t_record").tableInfo should be(AuditTableInfo("t_record", Seq("id"), Map.empty, true))

      spark.conf.set(UPDATE_TABLE_METADATA, true)
      val flow2 = Waimak.sparkFlow(spark)
        .getOrCreateAuditTable(testingBaseDirName, None)("t_record")

      intercept[DataFlowException](executor.execute(flow2)).cause.getMessage should be("spark.waimak.storage.updateMetadata is set to true but no metadata function was defined")
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

  describe("CompactionPartitioner") {

    it("TotalBytesPartitioner should create partition differently depending on total bytes") {
      val spark = sparkSession
      import spark.implicits._
      val context = new EmptyFlowContext

      val df = Seq.fill(10)(1).toDF()
      val rowSize = org.apache.spark.util.SizeEstimator.estimate(df.head().asInstanceOf[AnyRef])

      context.conf.setProperty(BYTES_PER_PARTITION, (5 * rowSize).toString)
      CompactionPartitionerGenerator.getImplementation(context)(df, 10) should be(2)

      context.conf.setProperty(BYTES_PER_PARTITION, (4 * rowSize).toString)
      CompactionPartitionerGenerator.getImplementation(context)(df, 10) should be(3)

      CompactionPartitionerGenerator.getImplementation(context)(df.filter(lit(false)), 0) should be(1)

    }

    it("TotalCellsPartitioner should create partition differently depending on total cells") {
      val spark = sparkSession
      import spark.implicits._
      val context = new EmptyFlowContext
      context.conf.setProperty(COMPACTION_PARTITIONER_IMPLEMENTATION, "com.coxautodata.waimak.storage.TotalCellsPartitioner")

      val df = Seq.fill(10)(1).toDF()

      context.conf.setProperty(CELLS_PER_PARTITION, "5")
      CompactionPartitionerGenerator.getImplementation(context)(df, 10) should be(2)

      context.conf.setProperty(CELLS_PER_PARTITION, "4")
      CompactionPartitionerGenerator.getImplementation(context)(df, 10) should be(3)

      CompactionPartitionerGenerator.getImplementation(context)(df.filter(lit(false)), 0) should be(1)

    }

  }
}

case class TRecord(id: Int, value: String, lastUpdated: Timestamp)
