package com.coxautodata.waimak.storage

import com.coxautodata.waimak.dataflow.spark.TestSparkData._
import com.coxautodata.waimak.dataflow.spark.{SparkAndTmpDirSpec, TPerson}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

import scala.util.Success

/**
  * Created by Alexei Perelighin on 2018/02/08
  */
class TestFileStorageOpsWithStaging extends SparkAndTmpDirSpec {

  override val appName: String = "File Ops tests"

  var basePath: Path = _

  var trashBinPath: Path = _

  var tempFolder: Path = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    basePath = new Path(tmpDir.toString + "/basePath")
    trashBinPath = new Path(tmpDir.toString + "/trashBin")
    tempFolder = new Path(testingBaseDir.toAbsolutePath.toString + "/.tmp")
  }

  def createFops(): FileStorageOps = new FileStorageOpsWithStaging(
    FileSystem.getLocal(sparkSession.sparkContext.hadoopConfiguration)
    , sparkSession
    , tempFolder
    , trashBinPath
  )

  describe("ops") {

    val tableName = "persons"

    it("write") {
      val spark = sparkSession
      import spark.implicits._

      val tablePath = new Path(basePath, tableName)
      val fops = createFops()

      val r1Data = persons.toDS()
      val r1Path = new Path(tablePath, "r1")

      fops.writeParquet(tableName, r1Path, r1Data)

      val back = spark.read.parquet(r1Path.toString).orderBy($"id")
      back.as[TPerson].collect() should be(persons)
    }

    it("write and read") {
      val spark = sparkSession
      import spark.implicits._

      val tablePath = new Path(basePath, tableName)
      val fops = createFops()

      val r1Data = persons.toDS()
      val r1Path = new Path(tablePath, "r1")

      fops.writeParquet(tableName, r1Path, r1Data)

      val back = fops.openParquet(r1Path).get.orderBy($"id")
      back.as[TPerson].collect() should be(persons)
    }

    it("swap") {
      val spark = sparkSession
      import spark.implicits._

      val tablePath = new Path(basePath, tableName)
      val fops = createFops()

      val r1Data = persons.toDS()
      fops.writeParquet(tableName, new Path(tablePath, "p=r1"), r1Data)
      fops.writeParquet(tableName, new Path(tablePath, "p=r2"), r1Data)
      fops.writeParquet(tableName, new Path(tablePath, "p=r3"), r1Data)

      val r2Data = persons_2.toDS()
      fops.atomicWriteAndCleanup(tableName, r2Data, new Path(tablePath, "p=c1"), tablePath, Seq("p=r1", "p=r2"))

      val updatedData = fops.openParquet(tablePath).get.groupBy("p").count().collect().map(r => (r.get(0), r.get(1))).toMap
      updatedData should be(Map("c1" -> 3, "r3" -> 5))

      val trashData = fops.openParquet(new Path(trashBinPath, tableName)).get.groupBy("p").count().collect().map(r => (r.get(0), r.get(1))).toMap
      trashData should be(Map("r1" -> 5, "r2" -> 5))
    }

    it("swap overwrite") {
      val spark = sparkSession
      import spark.implicits._

      val tablePath = new Path(basePath, tableName)
      val fops = createFops()

      val r1Data = persons.toDS()
      val r2Data = persons_2.toDS()

      fops.writeParquet(tableName, new Path(tablePath, "p=r1"), r1Data)
      fops.writeParquet(tableName, new Path(tablePath, "p=r2"), r1Data)
      fops.writeParquet(tableName, new Path(tablePath, "p=r3"), r1Data)
      fops.writeParquet(tableName, new Path(tablePath, "p=c1"), r1Data)

      fops.atomicWriteAndCleanup(tableName, r2Data, new Path(tablePath, "p=c1"), tablePath, Seq("p=r1", "p=r2"))

      val updatedData = fops.openParquet(tablePath).get.groupBy("p").count().collect().map(r => (r.get(0), r.get(1))).toMap
      updatedData should be(Map("c1" -> 3, "r3" -> 5))

      val trashData = fops.openParquet(new Path(trashBinPath, tableName)).get.groupBy("p").count().collect().map(r => (r.get(0), r.get(1))).toMap
      trashData should be(Map("r1" -> 5, "r2" -> 5))
    }

    it("open with schema evolution") {
      val spark = sparkSession
      import spark.implicits._

      val tablePath = new Path(basePath, tableName)
      val fops = createFops()

      val r1Data = persons.toDS()
      val r2Data = persons_2.toDS().withColumn("extra", lit(4))

      fops.writeParquet(tableName, new Path(tablePath, "p=r1"), r1Data)
      fops.writeParquet(tableName, new Path(tablePath, "p=r2"), r1Data)
      fops.writeParquet(tableName, new Path(tablePath, "p=r3"), r1Data)
      fops.writeParquet(tableName, new Path(tablePath, "p=c1"), r2Data)


      val readBack = fops.openParquet(tablePath).get.withColumn("extra", coalesce($"extra", lit(-1)))
        .groupBy("extra").count()
        .collect().map(r => (r.get(0), r.get(1))).toMap
      readBack should be(Map(-1 -> 15, 4 -> 3))
    }
  }

  describe("AuditTableInfo") {

    it("CRUD with empty meta") {
      val fops = createFops()

      val info = AuditTableInfo("test_1", Seq("key1", "key2"), Map.empty)
      fops.writeAuditTableInfo(basePath, info) should be(Success(info))

      val readBackInfo = fops.readAuditTableInfo(basePath, "test_1")
      readBackInfo should be(Success(info))
    }

    it("CRUD with meta") {
      val fops = createFops()

      val info = AuditTableInfo("test_1", Seq("key1", "key2"), Map("info1" -> "v1", "info2" -> "v2", "info3" -> "v3"))
      fops.writeAuditTableInfo(basePath, info) should be(Success(info))

      val readBackInfo = fops.readAuditTableInfo(basePath, "test_1")
      readBackInfo should be(Success(info))
    }
  }

  describe("listTables") {

    it("ignore hidden") {
      val fops = createFops()
      fops.mkdirs(new Path(basePath, ".tmp"))
      fops.mkdirs(new Path(basePath, ".Trash"))
      fops.mkdirs(new Path(basePath, "table_1"))
      fops.mkdirs(new Path(basePath, "table_2"))
      fops.mkdirs(new Path(basePath, "table_3"))

      val f2 = createFops()
      f2.listTables(basePath).sorted should be(Seq("table_1", "table_2", "table_3"))
    }

    it("list empty, base does not exist") {
      val fops = createFops()
      fops.listTables(basePath) should be(Seq.empty)
    }

    it("list empty, base does exists") {
      val fops = createFops()
      fops.mkdirs(basePath)
      fops.listTables(basePath) should be(Seq.empty)
    }
  }
}
