package com.coxautodata.waimak.storage

import java.io.File
import java.sql.Timestamp
import java.time.Duration

import com.coxautodata.waimak.dataflow.spark.TestSparkData._
import com.coxautodata.waimak.dataflow.spark.{SparkAndTmpDirSpec, TPersonEvolved}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import com.coxautodata.waimak.storage.AuditTableFile._
import org.apache.spark.sql.types.TimestampType

import scala.util.{Failure, Success}

/**
  * Created by Alexei Perelighin on 2018/03/08
  */
class TestAuditTableFile extends SparkAndTmpDirSpec {

  override val appName: String = "Audit Files"

  var basePath: Path = _

  var trashBinPath: Path = _

  var tempFolder: Path = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    basePath = new Path(testingBaseDir.toAbsolutePath.toString + "/basePath")
    trashBinPath = new Path(testingBaseDir.toAbsolutePath.toString + "/.trashBin")
    tempFolder = new Path(testingBaseDir.toAbsolutePath.toString + "/.tmp")
  }

  def sequence(): (AuditTableFile) => String = {
    var i = -1
    (_) => {
      i = i + 1
      i.toString
    }
  }

  def createFops(): FileStorageOps = new FileStorageOpsWithStaging(
    FileSystem.getLocal(sparkSession.sparkContext.hadoopConfiguration)
    , sparkSession
    , tempFolder
    , trashBinPath
  )

  def createADTable(tableName: String, fosp: FileStorageOps): AuditTableFile = {
    new AuditTableFile(AuditTableInfo(tableName, Seq("id"), Map.empty), Seq.empty, fosp, basePath, sequence())
  }

  val lastUpdated = (d: Dataset[_]) => unix_timestamp(d("lastTS"), "yyyy-MM-dd").cast(TimestampType)

  val compactTS_1 = new Timestamp(formatter.parse("2018-01-03 00:00").getTime)
  val t1 = new Timestamp(formatter.parse("2018-01-01 10:00").getTime)
  val t2 = new Timestamp(formatter.parse("2018-01-01 10:01").getTime)

  val d3d = Duration.ofDays(3)
  val d12h = Duration.ofHours(12)

  describe("ops") {

    val tableName = "person"

    it("init table success") {
      val zeroState = createADTable(tableName, createFops())

      val nextState = zeroState.initNewTable().get

      nextState.regions.size should be(0)
      nextState.getLatestTimestamp() should be(None)

      val tableFolder = new File(s"${basePath.toString}/${tableName}")
      tableFolder.exists() should be(true)
      tableFolder.list().sorted should be(Array("..table_info.crc", ".table_info", "de_store_type=cold", "de_store_type=hot"))

      val inferredRegions = AuditTableFile.inferRegionsWithStats(sparkSession, zeroState.storageOps, basePath, Seq(tableName))
      inferredRegions should be(Seq.empty)

      val infoData = zeroState.storageOps.readAuditTableInfo(basePath, tableName)
      infoData should be(Success(AuditTableInfo(tableName, Seq("id"), Map.empty)))
    }

    it("init table fail") {
      val zeroState = createADTable(tableName, createFops())

      val nextState = zeroState.initNewTable()

      val res = zeroState.initNewTable()

      res should be(Failure(StorageException(s"Table [person] already exists in path [${basePath.toString}/person]")))
    }

    it("init table fail, no keys") {

      val zeroState = new AuditTableFile(AuditTableInfo(tableName, Seq.empty, Map.empty), Seq.empty, createFops(), basePath, sequence())

      val res = zeroState.initNewTable()

      res should be(Failure(StorageException("Table [person] must have at least one column in primary keys.")))
    }

    it("append table") {
      val spark = sparkSession
      import spark.implicits._

      val table = createADTable(tableName, createFops()).initNewTable().get
      val r1Data = persons.toDS().withColumn("lastTS", lit("2018-01-01"))

      val (table_s11, cs1) = table.append(r1Data, lastUpdated(r1Data), t1).get
      val table_s1 = table_s11.asInstanceOf[AuditTableFile]
      cs1 should be(5)

      table_s1.regions.size should be(1)
      table_s1.regions(0).count should be(5)
      table_s1.regions(0).max_last_updated should be(lastTS_1)

      table_s1.getLatestTimestamp() should be(Some(lastTS_1))

      val regionFolder = new File(s"${basePath.toString}/person/de_store_type=hot/de_store_region=0")
      regionFolder.exists() should be(true)

      (new File(s"${basePath.toString}/person/de_store_type=cold")).list().length should be(0)

      val resData = spark.read.parquet(regionFolder.toString)
      resData.schema.fieldNames.sorted should be(Array("_de_last_updated", "country", "id", "lastTS", "name"))
      resData.count() should be(5)

      val inferredRegionsCached = AuditTableFile.inferRegionsWithStats(sparkSession, table.storageOps, basePath, Seq(tableName))
      inferredRegionsCached should be(Seq(AuditTableRegionInfo("person", "hot", "0", t1, false, 5, lastTS_1)))

      val inferredRegionsNoCache = AuditTableFile.inferRegionsWithStats(sparkSession, table.storageOps, basePath, Seq(tableName), skipRegionInfoCache = true)
      inferredRegionsNoCache should be(Seq(AuditTableRegionInfo("person", "hot", "0", lowTimestamp, false, 5, lastTS_1)))
    }

    it("append multiple times with compaction") {
      val spark = sparkSession
      import spark.implicits._

      val table = createADTable(tableName, createFops()).initNewTable().get
      val r1Data = persons.toDS().withColumn("lastTS", lit("2018-01-01"))
      val r2Data = persons_2.toDS().withColumn("lastTS", lit("2018-01-02")).withColumn("schema_evolution", lit(9))
      val r3Data = persons_3.toDS().withColumn("lastTS", lit("2018-01-03"))
      val emptyData = spark.createDataFrame(spark.sparkContext.parallelize(Seq.empty[Row]), r3Data.schema)

      val table_empty = table.append(emptyData, lit(lowTimestamp), t1).get._1
      val table_s1 = table_empty.append(r1Data, lastUpdated(r1Data), t1).get._1
      val table_s2 = table_s1.append(r2Data, lastUpdated(r2Data), t2).get._1.asInstanceOf[AuditTableFile]

      table_s2.regions.size should be(3)
      table_s2.regions(0).count should be(0)
      table_s2.regions(0).store_type should be("hot")
      table_s2.regions(0).created_on should be(t1)
      table_s2.regions(0).is_deprecated should be(false)
      table_s2.regions(0).max_last_updated should be(lowTimestamp)

      table_s2.regions(1).count should be(5)
      table_s2.regions(1).store_type should be("hot")
      table_s2.regions(1).created_on should be(t1)
      table_s2.regions(1).is_deprecated should be(false)
      table_s2.regions(1).max_last_updated should be(lastTS_1)

      table_s2.regions(2).count should be(3)
      table_s2.regions(2).store_type should be("hot")
      table_s2.regions(2).created_on should be(t2)
      table_s2.regions(2).is_deprecated should be(false)
      table_s2.regions(2).max_last_updated should be(lastTS_2)

      table_s2.getLatestTimestamp() should be(Some(lastTS_2))

      val regionFolder_0 = new File(s"${basePath.toString}/person/de_store_type=hot/de_store_region=0")
      regionFolder_0.exists() should be(true)

      val regionFolder_1 = new File(s"${basePath.toString}/person/de_store_type=hot/de_store_region=1")
      regionFolder_1.exists() should be(true)

      val regionFolder_2 = new File(s"${basePath.toString}/person/de_store_type=hot/de_store_region=2")
      regionFolder_2.exists() should be(true)

      (new File(s"${basePath.toString}/person/de_store_type=cold")).list().length should be(0)

      val resData_0 = spark.read.parquet(regionFolder_0.toString)
      resData_0.schema.fieldNames.sorted should be(Array("_de_last_updated", "country", "id", "lastTS", "name"))
      resData_0.count() should be(0)

      val resData_1 = spark.read.parquet(regionFolder_1.toString)
      resData_1.schema.fieldNames.sorted should be(Array("_de_last_updated", "country", "id", "lastTS", "name"))
      resData_1.count() should be(5)
      resData_1.select("_de_last_updated").distinct().collect()(0).getAs[Timestamp](0) should be(lastTS_1)

      val resData_2 = spark.read.parquet(regionFolder_2.toString)
      resData_2.schema.fieldNames.sorted should be(Array("_de_last_updated", "country", "id", "lastTS", "name", "schema_evolution"))
      resData_2.count() should be(3)
      resData_2.select("_de_last_updated").distinct().collect()(0).getAs[Timestamp](0) should be(lastTS_2)

      val inferredRegions = AuditTableFile.inferRegionsWithStats(sparkSession, table.storageOps, basePath, Seq(tableName)).sortBy(_.store_region)
      inferredRegions should be(Seq(
        AuditTableRegionInfo("person", "hot", "0", t1, false, 0, lowTimestamp)
        , AuditTableRegionInfo("person", "hot", "1", t1, false, 5, lastTS_1)
        , AuditTableRegionInfo("person", "hot", "2", t2, false, 3, lastTS_2)
      )
      )

      val onlyColdRegion_empty = AuditTableFile.inferRegionsWithStats(sparkSession, table.storageOps, basePath, Seq(tableName), false).sortBy(_.store_region)
      onlyColdRegion_empty should be(Seq.empty)

      val compactedTable = table_s2.compact(lastTS_3, d3d).get

      val onlyColdRegion = AuditTableFile.inferRegionsWithStats(sparkSession, table.storageOps, basePath, Seq(tableName), false).sortBy(_.store_region)
      onlyColdRegion should be(Seq(AuditTableRegionInfo("person", "cold", "3", lastTS_3, false, 8, lastTS_2)))

      val (table_s31, cs3) = compactedTable.append(r3Data, lastUpdated(r3Data), lastTS_3).get
      val table_s3 = table_s31.asInstanceOf[AuditTableFile]
      cs3 should be(3)

      table_s3.regions.size should be(2)
      table_s3.regions(0).store_region should be("3") // The cold region would not be recompacted again
      table_s3.regions(1).store_region should be("4")

      val cold = AuditTableFile.inferRegionsWithStats(sparkSession, table.storageOps, basePath, Seq(tableName), false).sortBy(_.store_region)
      cold.map(_.store_type) should be(Seq("cold"))

      val coldHot = AuditTableFile.inferRegionsWithStats(sparkSession, table.storageOps, basePath, Seq(tableName), true).sortBy(_.store_region)
      coldHot.map(_.store_type).sorted should be(Seq("cold", "hot"))

      // Should be one trashed compaction
      new File(trashBinPath.toString, tableName).list() should contain theSameElementsAs Seq(lastTS_3.getTime.toString)

      val secondCompact = table_s3.compact(lastTS_4, d12h).get
      secondCompact.regions.size should be(1)
      secondCompact.regions(0).store_region should be("6")

      // Should be a different trashed compaction, original one would be deleted
      new File(trashBinPath.toString, tableName).list() should contain theSameElementsAs Seq(lastTS_4.getTime.toString)

      // Empty compaction with previous in range should not delete trash
      val thirdCompact = secondCompact.compact(lastTS_5, d3d).get
      thirdCompact.regions.size should be(1)
      thirdCompact.regions(0).store_region should be("6")
      new File(trashBinPath.toString, tableName).list() should contain theSameElementsAs Seq(lastTS_4.getTime.toString)

      // Empty compaction with previous not in range should delete trash
      val fourthCompact = thirdCompact.compact(lastTS_6, d12h).get
      fourthCompact.regions.size should be(1)
      fourthCompact.regions(0).store_region should be("6")
      new File(trashBinPath.toString, tableName).list() should contain theSameElementsAs Seq()

    }

    it("single compact into new with schema evolution on hot") {
      val spark = sparkSession
      import spark.implicits._

      val table = createADTable(tableName, createFops()).initNewTable().get
      val r1Data = persons.toDS().withColumn("lastTS", lit("2018-01-01"))
      val r2Data = persons_2.toDS().withColumn("lastTS", lit("2018-01-02")).withColumn("schema_evolution", lit(9))

      val table_s1 = table.append(r1Data, lastUpdated(r1Data), t1).get._1
      val table_s2 = table_s1.append(r2Data, lastUpdated(r2Data), t2).get._1.asInstanceOf[AuditTableFile]

      table_s2.regions.size should be(2)
      table_s2.regions(0).store_region should be("0")
      table_s2.regions(1).store_region should be("1")

      val compacted_table = table_s2.compact(compactTS_1, d3d).get
      compacted_table.regions.size should be(1)
      compacted_table.regions(0).count should be(8)
      compacted_table.regions(0).store_type should be("cold")
      compacted_table.regions(0).created_on should be(compactTS_1)
      compacted_table.regions(0).is_deprecated should be(false)
      compacted_table.regions(0).max_last_updated should be(lastTS_2)
      compacted_table.regions(0).store_region should be("2") //first hot will be compacted into cold, cold will not be re-compacted

      compacted_table.getLatestTimestamp() should be(Some(lastTS_2))

      //Nothing in the hot partitions
      (new File(s"${basePath.toString}/person/de_store_type=hot")).list().length should be(0)
      val compactedPath_1 = new File(s"${basePath.toString}/person/de_store_type=cold/de_store_region=2")

      //Testing compacted data
      val compacted_1 = spark.read.parquet(compactedPath_1.toString)
      compacted_1.schema.fieldNames.sorted should be(Array("_de_last_updated", "country", "id", "lastTS", "name", "schema_evolution"))
      compacted_1.count() should be(8)
      val lastCount = compacted_1.groupBy($"_de_last_updated").count().collect().map(r => (r.get(0), r.get(1))).toMap
      lastCount should be(Map(lastTS_1 -> 5, lastTS_2 -> 3))
      compacted_1.orderBy($"id", $"_de_last_updated").as[TPersonEvolved].collect() should be(persons_evolved_compacted_1.sortBy(r => (r.id, r.lastTS)))

      val trashFolder = new File(s"${trashBinPath.toString}/person/${compactTS_1.getTime.toString}") //cold or hot is not carried over, just region ids
      trashFolder.list().sorted should be(Array("de_store_region=0", "de_store_region=1"))

      val inferredRegions = AuditTableFile.inferRegionsWithStats(sparkSession, table.storageOps, basePath, Seq(tableName))
      inferredRegions should be(Seq(AuditTableRegionInfo("person", "cold", "2", compactTS_1, false, 8, lastTS_2)))
    }

    it("fail to append twice") {
      val spark = sparkSession
      import spark.implicits._

      val table = createADTable(tableName, createFops()).initNewTable().get
      val r1Data = persons.toDS().withColumn("lastTS", lit("2018-01-01"))
      val r2Data = persons_2.toDS().withColumn("lastTS", lit("2018-01-02")).withColumn("schema_evolution", lit(9))

      table.append(r1Data, lastUpdated(r1Data), t1).get

      val error = table.append(r2Data, lastUpdated(r2Data), t2)

      error should be(Failure(StorageException(s"Table [$tableName] can no longer be updated.")))
    }

    it("should regenerate region cache after compaction") {
      val spark = sparkSession
      import spark.implicits._

      val person = createADTable("prsn", createFops()).initNewTable().get


      val r1Data = persons.toDS().withColumn("lastTS", lit("2018-01-01"))
      val r2Data = persons_2.toDS().withColumn("lastTS", lit("2018-01-02")).withColumn("schema_evolution", lit(9))
      val r3Data = persons_3.toDS().withColumn("lastTS", lit("2018-01-03"))

      val finalPerson = person
        .append(r1Data, lastUpdated(r1Data), lastTS_1)
        .flatMap(_._1.append(r2Data, lastUpdated(r2Data), lastTS_2))
        .flatMap(_._1.compact(lastTS_2, d3d))
        .flatMap(_.append(r3Data, lastUpdated(r3Data), lastTS_3))
        .get._1.asInstanceOf[AuditTableFile]

      val inferredCachedRegions = AuditTableFile.inferRegionsFromCache(person.storageOps, basePath, Seq("prsn"), true)
      inferredCachedRegions should contain theSameElementsAs Seq(
        AuditTableRegionInfo("prsn", "cold", "2", lastTS_2, false, 8, lastTS_2),
        AuditTableRegionInfo("prsn", "hot", "3", lastTS_3, false, 3, lastTS_3))

      val fs = FileSystem.getLocal(sparkSession.sparkContext.hadoopConfiguration)
      val prsnCachePath = new Path(new Path(basePath, AuditTableFile.REGION_INFO_DIRECTORY), "prsn")
      fs.exists(prsnCachePath) should be(true)
      fs.delete(prsnCachePath, true) should be(true)

      val inferredCachedRegionsAfterDelete = AuditTableFile.inferRegionsFromCache(person.storageOps, basePath, Seq("prsn"), true)
      inferredCachedRegionsAfterDelete should be(Seq.empty)

      val inferredAllRegions = AuditTableFile.inferRegionsWithStats(sparkSession, person.storageOps, basePath, Seq("prsn"), true)
      inferredAllRegions should contain theSameElementsAs Seq(
        AuditTableRegionInfo("prsn", "cold", "2", lowTimestamp, false, 8, lastTS_2),
        AuditTableRegionInfo("prsn", "hot", "3", lowTimestamp, false, 3, lastTS_3)
      )

      val reopenedTable = AuditTableFile.openTables(sparkSession, finalPerson.storageOps, basePath, Seq("prsn"), true)(finalPerson.newRegionID)._1("prsn")
      reopenedTable.map(_.compact(lastTS_3, d3d))

      val inferredCachedRegionsAfterCompaction = AuditTableFile.inferRegionsFromCache(person.storageOps, basePath, Seq("prsn"), true)
      inferredCachedRegionsAfterCompaction should be(Seq(AuditTableRegionInfo("prsn", "cold", "5", lastTS_3, false, 11, lastTS_3)))

      val inferredAllRegionsAfterCompaction = AuditTableFile.inferRegionsWithStats(sparkSession, person.storageOps, basePath, Seq("prsn"), true)
      inferredAllRegionsAfterCompaction should be(Seq(AuditTableRegionInfo("prsn", "cold", "5", lastTS_3, false, 11, lastTS_3)))

    }
  }

  describe("openTables") {

    it("open table without regions") {
      val spark = sparkSession

      val table = createADTable("prsn", createFops()).initNewTable().get

      val (tables, missing) = AuditTableFile.openTables(sparkSession, table.storageOps, basePath, Seq("prsn", "missing"), true)(table.newRegionID)

      tables.size should be(1)
      tables.get("prsn").map(_.get.tableName) should be(Some("prsn"))
      tables.get("prsn").map(_.get.regions.size) should be(Some(0))
      tables.get("prsn").map(_.get.tableInfo) should be(Some(AuditTableInfo("prsn", Seq("id"), Map.empty)))

      missing should be(Seq("missing"))
    }

    it("open 2 tables with regions") {
      val spark = sparkSession
      import spark.implicits._

      val person = createADTable("prsn", createFops()).initNewTable().get
      val reportTable = createADTable("rep", createFops()).initNewTable().get


      val r1Data = persons.toDS().withColumn("lastTS", lit("2018-01-01"))
      val r2Data = persons_2.toDS().withColumn("lastTS", lit("2018-01-02")).withColumn("schema_evolution", lit(9))
      val r3Data = persons_3.toDS().withColumn("lastTS", lit("2018-01-03"))
      val rep1 = report.toDS().withColumn("lastTS", lit("2018-01-01"))

      person
        .append(r1Data, lastUpdated(r1Data), lastTS_1)
        .flatMap(_._1.append(r2Data, lastUpdated(r2Data), lastTS_2))
        .flatMap(_._1.compact(lastTS_2, d3d))
        .flatMap(_.append(r3Data, lastUpdated(r3Data), lastTS_3))

      reportTable.append(rep1, lastUpdated(rep1), lastTS_2)

      val (one, _) = AuditTableFile.openTables(sparkSession, person.storageOps, basePath, Seq("prsn", "missing"), true)(person.newRegionID)
      one.size should be(1)
      one.get("prsn").map(_.get.tableName) should be(Some("prsn"))

      val (two, missing) = AuditTableFile.openTables(sparkSession, person.storageOps, basePath, Seq("prsn", "rep", "missing"), true)(person.newRegionID)
      missing should be(Seq("missing"))
      two.size should be(2)
      two.get("prsn").map(_.get.tableName) should be(Some("prsn"))
      two.get("prsn").map(_.get.regions.filter(_.store_type == "cold").size) should be(Some(1))
      two.get("prsn").map(_.get.regions.filter(_.store_type == "hot").size) should be(Some(1))

      two.get("rep").map(_.get.tableName) should be(Some("rep"))
      two.get("rep").map(_.get.regions.filter(_.store_type == "cold").size) should be(Some(0))
      two.get("rep").map(_.get.regions.filter(_.store_type == "hot").size) should be(Some(1))

    }

    it("should deal with a mixture of existing and new tables") {
      val table1 = createADTable("table_1", createFops()).initNewTable().get
      val table2 = createADTable("table_2", createFops()).initNewTable().get

      val res = AuditTableFile.openTables(sparkSession, table1.storageOps, basePath, Seq("table_1", "does_not_exist", "table_2"), true)(table1.newRegionID)
      println(res)
      res._1.map(_._1).toSet should be(Set("table_1", "table_2"))
      res._2 should be(Seq("does_not_exist"))
    }

    it("should deal with cached and non-cached region info") {
      val spark = sparkSession
      import spark.implicits._

      val person = createADTable("prsn", createFops()).initNewTable().get
      val reportTable = createADTable("rep", createFops()).initNewTable().get


      val r1Data = persons.toDS().withColumn("lastTS", lit("2018-01-01"))
      val r2Data = persons_2.toDS().withColumn("lastTS", lit("2018-01-02")).withColumn("schema_evolution", lit(9))
      val r3Data = persons_3.toDS().withColumn("lastTS", lit("2018-01-03"))
      val rep1 = report.toDS().withColumn("lastTS", lit("2018-01-01"))

      val finalPerson = person
        .append(r1Data, lastUpdated(r1Data), lastTS_1)
        .flatMap(_._1.append(r2Data, lastUpdated(r2Data), lastTS_2))
        .flatMap(_._1.compact(lastTS_2, d3d))
        .flatMap(_.append(r3Data, lastUpdated(r3Data), lastTS_3))

      val finalReport = reportTable.append(rep1, lastUpdated(rep1), lastTS_2)

      val inferredCachedRegions = AuditTableFile.inferRegionsFromCache(person.storageOps, basePath, Seq("prsn", "rep"), true)
      inferredCachedRegions should contain theSameElementsAs Seq(
        AuditTableRegionInfo("prsn", "cold", "2", lastTS_2, false, 8, lastTS_2),
        AuditTableRegionInfo("prsn", "hot", "3", lastTS_3, false, 3, lastTS_3),
        AuditTableRegionInfo("rep", "hot", "0", lastTS_2, false, 5, lastTS_1))


      val fs = FileSystem.getLocal(sparkSession.sparkContext.hadoopConfiguration)
      val rep1CachePath = new Path(new Path(basePath, AuditTableFile.REGION_INFO_DIRECTORY), "rep")
      fs.exists(rep1CachePath) should be(true)
      fs.delete(rep1CachePath, true) should be(true)

      val inferredCachedRegionsAfterDelete = AuditTableFile.inferRegionsFromCache(person.storageOps, basePath, Seq("prsn", "rep"), true)
      inferredCachedRegionsAfterDelete should contain theSameElementsAs Seq(
        AuditTableRegionInfo("prsn", "cold", "2", lastTS_2, false, 8, lastTS_2),
        AuditTableRegionInfo("prsn", "hot", "3", lastTS_3, false, 3, lastTS_3))


      val inferredAllRegions = AuditTableFile.inferRegionsWithStats(sparkSession, person.storageOps, basePath, Seq("prsn", "rep"), true)
      inferredAllRegions should contain theSameElementsAs Seq(
        AuditTableRegionInfo("prsn", "cold", "2", lastTS_2, false, 8, lastTS_2),
        AuditTableRegionInfo("prsn", "hot", "3", lastTS_3, false, 3, lastTS_3),
        AuditTableRegionInfo("rep", "hot", "0", lowTimestamp, false, 5, lastTS_1)
      )

    }
  }

  describe("nextLongRegion") {

    it("empty") {
      val table = createADTable("t1", createFops())
      AuditTableFile.nextLongRegion(table) should be("r00000000000000000000")
    }

    it("next out of one") {
      val table = createADTable("t1", createFops())
      val withOne = AuditTableFile.setRegions(table, Seq(AuditTableRegionInfo("person", "hot", "r00000000000000000011", lowTimestamp, false, 5, lastTS_1)))
      AuditTableFile.nextLongRegion(withOne) should be("r00000000000000000012")
    }

    it("next out of 2") {
      val table = createADTable("t1", createFops())
      val withOne = AuditTableFile.setRegions(table, Seq(
        AuditTableRegionInfo("person", "hot", "r00000000000000000111", lowTimestamp, false, 5, lastTS_1)
        , AuditTableRegionInfo("person", "hot", "r00000000000000000011", lowTimestamp, false, 5, lastTS_1)))
      AuditTableFile.nextLongRegion(withOne) should be("r00000000000000000112")
    }
  }
}
