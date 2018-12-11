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

    it("stage csv to parquet and commit to impala") {
      val spark = sparkSession
      val executor = Waimak.sparkExecutor()

      val connector1 = ImpalaDummyConnector(spark)
      val connector2 = ImpalaDummyConnector(spark)
      val connectorRecreate = ImpalaDummyConnector(spark, forceRecreateTables = true)

      val baseDest = testingBaseDir + "/dest"

      val flow = SparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "items")
        .alias("csv_2", "person")
        .alias("csv_2", "person_recreate")
        .commit("connector1", partitions = Seq("amount"))("items")
        .commit("connector2WithSnapshot")("person")
        .commit("connectorRecreateWithSnapshot")("person_recreate")
        .push("connector1")(ParquetDataCommitter(baseDest).withHadoopDBConnector(connector1))
        .push("connector2WithSnapshot")(ParquetDataCommitter(baseDest).withHadoopDBConnector(connector2).withSnapshotFolder("generatedTimestamp=2018-03-13-16-19-00"))
        .push("connectorRecreateWithSnapshot")(ParquetDataCommitter(baseDest).withHadoopDBConnector(connectorRecreate).withSnapshotFolder("generatedTimestamp=2018-03-13-16-19-00"))

      val (_, finalState) = executor.execute(flow)
      finalState.inputs.size should be(5)

      val itemsParquet = new File(testingBaseDirName, "dest/items/amount=1").list().filter(_.endsWith(".parquet")).head
      val personParquet = new File(testingBaseDirName, "dest/person/generatedTimestamp=2018-03-13-16-19-00").list().filter(_.endsWith(".parquet")).head
      val person_recreateParquet = new File(testingBaseDirName, "dest/person_recreate/generatedTimestamp=2018-03-13-16-19-00").list().filter(_.endsWith(".parquet")).head

      connector1.ranDDLs should be {
        List(List(
          "drop table if exists items",
          s"create external table if not exists items like parquet 'file:$testingBaseDirName/dest/items/amount=1/$itemsParquet' partitioned by (amount string) stored as parquet location '$testingBaseDirName/dest/items'",
          "alter table items recover partitions"
        ))
      }

      connector2.ranDDLs should be {
        List(List(
          s"create external table if not exists person like parquet 'file:$testingBaseDirName/dest/person/generatedTimestamp=2018-03-13-16-19-00/$personParquet' stored as parquet location '$testingBaseDir/dest/person/generatedTimestamp=2018-03-13-16-19-00'",
          s"alter table person set location '$testingBaseDirName/dest/person/generatedTimestamp=2018-03-13-16-19-00'"
        ))
      }

      connectorRecreate.ranDDLs should be {
        List(List(
          "drop table if exists person_recreate",
          s"create external table if not exists person_recreate like parquet 'file:$testingBaseDirName/dest/person_recreate/generatedTimestamp=2018-03-13-16-19-00/$person_recreateParquet' stored as parquet location '$testingBaseDir/dest/person_recreate/generatedTimestamp=2018-03-13-16-19-00'"
        ))
      }
    }


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

      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).withSnapshotFolder("snapshot=20181101_123001_567")))
      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).withSnapshotFolder("snapshot=20181101_123001_568")))
      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).withSnapshotFolder("snapshot=20181103_123001_567")))
      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).withSnapshotFolder("snapshot=20181103_123001_568")))
      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).withSnapshotFolder("snapshot=20181105_123001_567")))
      executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).withSnapshotFolder("snapshot=20181105_123001_568")))

      executor.execute(flowPrePush.push("comm_1")(
        ParquetDataCommitter(baseDest)
          .withSnapshotFolder("snapshot=20181105_123001_569")
          .withDateBasedSnapshotCleanup("snapshot", "yyyyMMdd_HHmmss_SSS", 3)
          .withHadoopDBConnector(connectorRecreate)
      )
      )

      flowPrePush.flowContext.fileSystem.listStatus(new Path(baseDest + "/purchases")).map(_.getPath.getName).sorted should be(Array("snapshot=20181105_123001_567", "snapshot=20181105_123001_568", "snapshot=20181105_123001_569"))
      flowPrePush.flowContext.fileSystem.listStatus(new Path(baseDest + "/buyers")).map(_.getPath.getName).sorted should be(Array("snapshot=20181105_123001_567", "snapshot=20181105_123001_568", "snapshot=20181105_123001_569"))

      connectorRecreate.ranDDLs.size should be(1)
      connectorRecreate.ranDDLs(0).size should be(4)
    }

  }
}