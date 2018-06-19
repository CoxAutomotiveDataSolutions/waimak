package com.coxautodata.waimak.metastore

import java.io.File

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.TestSparkData.basePath
import com.coxautodata.waimak.dataflow.spark.{SimpleSparkDataFlow, SparkAndTmpDirSpec, SparkFlowContext}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import com.coxautodata.waimak.dataflow.spark.SparkActions._

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

  describe("stageAndCommitParquetToDB") {

    it("stage csv to parquet and commit to impala") {
      val spark = sparkSession
      import spark.implicits._
      val executor = Waimak.sparkExecutor()

      val connector = ImpalaDummyConnector(spark)
      val connectorRecreate = ImpalaDummyConnector(spark, forceRecreateTables = true)

      val baseDest = testingBaseDir + "/dest"

      val flow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "items")
        .alias("csv_2", "person")
        .alias("csv_2", "person_recreate")
        .stageAndCommitParquetToDB(connector)(baseDest, partitions = Seq("amount"))("items")
        .stageAndCommitParquetToDB(connector)(baseDest, snapshotFolder = Some("generatedTimestamp=2018-03-13-16-19-00"))("person")
        .stageAndCommitParquetToDB(connectorRecreate)(baseDest, snapshotFolder = Some("generatedTimestamp=2018-03-13-16-19-00"))("person_recreate")

      val (executedActions, finalState) = executor.execute(flow)
      finalState.inputs.size should be(5)

      val itemsParquet = new File(testingBaseDirName, "dest/items/amount=1").list().filter(_.endsWith(".parquet")).head
      val personParquet = new File(testingBaseDirName, "dest/person/generatedTimestamp=2018-03-13-16-19-00").list().filter(_.endsWith(".parquet")).head
      val person_recreateParquet = new File(testingBaseDirName, "dest/person_recreate/generatedTimestamp=2018-03-13-16-19-00").list().filter(_.endsWith(".parquet")).head

      connector.ranDDLs should be {
        List(
          "drop table if exists items",
          s"create external table if not exists items like parquet 'file:$testingBaseDirName/dest/items/amount=1/$itemsParquet' partitioned by (amount string) stored as parquet location '$testingBaseDirName/dest/items'",
          "alter table items recover partitions",
          s"create external table if not exists person like parquet 'file:$testingBaseDirName/dest/person/generatedTimestamp=2018-03-13-16-19-00/$personParquet' stored as parquet location '$testingBaseDir/dest/person/generatedTimestamp=2018-03-13-16-19-00'",
          s"alter table person set location '$testingBaseDirName/dest/person/generatedTimestamp=2018-03-13-16-19-00'"
        )
      }

      connectorRecreate.ranDDLs should be {
        List(
          "drop table if exists person_recreate",
          s"create external table if not exists person_recreate like parquet 'file:$testingBaseDirName/dest/person_recreate/generatedTimestamp=2018-03-13-16-19-00/$person_recreateParquet' stored as parquet location '$testingBaseDir/dest/person_recreate/generatedTimestamp=2018-03-13-16-19-00'"
        )
      }
    }
  }

}
