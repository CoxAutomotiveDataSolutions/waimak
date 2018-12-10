package com.coxautodata.waimak.metastore

import java.io.File

import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.TestSparkData.basePath
import com.coxautodata.waimak.dataflow.spark._
import com.coxautodata.waimak.dataflow.{DFExecutorPriorityStrategies, Waimak}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

class TestImpalaDBConnector extends SparkAndTmpDirSpec {

  var impalaConnection: HadoopDBConnector = _
  override val appName: String = "Metastore Utils"

  lazy val spark: SparkSession = sparkSession

  describe("ImpalaTestConnector") {

    it("should generate a correct drop table schema") {
      val impalaConnection: HadoopDBConnector = ImpalaDummyConnector(spark)
      impalaConnection.dropTableParquetDDL("testTable") should be("drop table if exists testTable")
    }

    it("should generate a correct update table path schema") {
      val impalaConnection: HadoopDBConnector = ImpalaDummyConnector(spark)
      impalaConnection.updateTableLocationDDL("testTable", "path") should be("alter table testTable set location 'path'")
    }

    it("should generate correct create table statements for non partitioned tables") {
      val impalaConnection: HadoopDBConnector = ImpalaDummyConnector(spark)
      val tableName = "testTable"
      val testingBaseFile = new File(testingBaseDirName)
      val tablePath = new File(testingBaseFile, tableName)
      tablePath.mkdirs()
      val testTableParquet = new File(tablePath, "part-0.parquet")
      FileUtils.touch(testTableParquet)

      //Test non-partition table
      impalaConnection.createTableFromParquetDDL(tableName, tablePath.toURI.getPath) should be(
        List(s"create external table if not exists $tableName like parquet " +
          s"'file:$testingBaseDirName/testTable/part-0.parquet' stored as " +
          s"parquet location '$testingBaseDirName/testTable/'")
      )
    }

    it("should generate correct create table statements for partitioned tables") {
      val impalaConnection: HadoopDBConnector = ImpalaDummyConnector(spark)
      val tableName = "testTable"
      val partitionName = "testPartition"
      val partitionFolder = s"$partitionName=value"
      val testingBaseFile = new File(testingBaseDirName)
      val tablePath = new File(testingBaseFile, tableName)
      val fullPartitionPath = new File(tablePath, partitionFolder)
      fullPartitionPath.mkdirs()
      val testTableParquet = new File(fullPartitionPath, "part-0.parquet")
      FileUtils.touch(testTableParquet)

      //Test partitioned table
      impalaConnection.createTableFromParquetDDL(tableName, tablePath.toURI.getPath, partitionColumns = Seq(partitionName)) should be(
        List(s"create external table if not exists $tableName like parquet " +
          s"'file:$testingBaseDirName/testTable/$partitionFolder/part-0.parquet' " +
          s"partitioned by ($partitionName string) " +
          s"stored as " +
          s"parquet location '$testingBaseDirName/testTable/'",
          s"alter table $tableName recover partitions")
      )
    }

  }

  describe("with snapshots, with DB commits") {

    it("multiple tables, multiple snapshots, with cleaning and DB commit") {
      val spark = sparkSession

      val executor = Waimak.sparkExecutor(1, DFExecutorPriorityStrategies.preferLoaders)

      val baseDest = testingBaseDir + "/dest"

      val connectorRecreate = ImpalaDummyConnector(spark, forceRecreateTables = true)

      val flowPrePush: SparkDataFlow = Waimak.sparkFlow(spark, tmpDir.toString)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "purchases")
        .alias("csv_2", "buyers")
        .commit("comm_1")("purchases", "buyers")

      flowPrePush.flowContext.fileSystem.exists(new Path(baseDest)) should be(false)

      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181101_123001_567")))
      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181101_123001_568")))
      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181103_123001_567")))
      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181103_123001_568")))
      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181105_123001_567")))
      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181105_123001_568")))

      executor.execute(flowPrePush.push("comm_1")(
        ParquetDataCommitter(baseDest)
          .snapshotFolder("snapshot=20181105_123001_569")
          .dateBasedSnapshotCleanup("snapshot", "yyyyMMdd_HHmmss_SSS", 3)
          .connection(connectorRecreate)
      )
      )

      flowPrePush.flowContext.fileSystem.listStatus(new Path(baseDest + "/purchases")).map(_.getPath.getName).sorted should be(Array("snapshot=20181105_123001_567", "snapshot=20181105_123001_568", "snapshot=20181105_123001_569"))
      flowPrePush.flowContext.fileSystem.listStatus(new Path(baseDest + "/buyers")).map(_.getPath.getName).sorted should be(Array("snapshot=20181105_123001_567", "snapshot=20181105_123001_568", "snapshot=20181105_123001_569"))

      connectorRecreate.ranDDLs.size should be(1)
      connectorRecreate.ranDDLs(0).size should be(4)
    }

  }
}
