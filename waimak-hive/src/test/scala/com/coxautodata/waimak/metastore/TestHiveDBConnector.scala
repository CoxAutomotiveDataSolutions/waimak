package com.coxautodata.waimak.metastore

import java.io.File

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.TestSparkData.basePath
import com.coxautodata.waimak.dataflow.spark.{SimpleSparkDataFlow, SparkAndTmpDirSpec, SparkFlowContext}
import org.apache.spark.sql.SparkSession

class TestHiveDBConnector extends SparkAndTmpDirSpec {

  var hiveConnection: HadoopDBConnector = _
  override val appName: String = "Metastore Utils"

  def spark: SparkSession = sparkSession

  describe("HiveTestConnector") {

    it("should generate a correct drop table schema") {
      val hiveConnection: HadoopDBConnector = HiveDummyConnector(SparkFlowContext(spark))
      hiveConnection.dropTableParquetDDL("testTable") should be("drop table if exists testTable")
    }

    it("should generate a correct update table path schema") {
      val hiveConnection: HadoopDBConnector = HiveDummyConnector(SparkFlowContext(spark))
      hiveConnection.updateTableLocationDDL("testTable", "/path") should be("alter table testTable set location 'file:/path'")
    }

    it("should generate correct create table statements for non partitioned tables") {
      val hiveConnection: HadoopDBConnector = HiveDummyConnector(SparkFlowContext(spark))
      val tableName = "testTable"
      val testingBaseFile = new File(testingBaseDirName)
      val tablePath = new File(testingBaseFile, tableName)

      spark.read.option("inferSchema", "true").option("header", "true").csv(s"$basePath/csv_1").write.parquet(tablePath.toString)

      //Test non-partition table
      hiveConnection.createTableFromParquetDDL(tableName, tablePath.toURI.getPath) should be(
        List(s"create external table if not exists $tableName " +
          "(id integer, item integer, amount integer) stored as " +
          s"parquet location 'file:$testingBaseDirName/testTable'")
      )
    }

    it("should generate correct create table statements for partitioned tables") {
      val hiveConnection: HadoopDBConnector = HiveDummyConnector(SparkFlowContext(spark))
      val tableName = "testTable"
      val partitionName = "amount"
      val testingBaseFile = new File(testingBaseDirName)
      val tablePath = new File(testingBaseFile, tableName)

      spark.read.option("inferSchema", "true").option("header", "true").csv(s"$basePath/csv_1").write.partitionBy(partitionName).parquet(tablePath.toString)

      //Test partitioned table
      hiveConnection.createTableFromParquetDDL(tableName, tablePath.toURI.getPath, partitionColumns = Seq(partitionName)) should be(
        List(s"create external table if not exists $tableName " +
          "(id integer, item integer)" +
          s" partitioned by ($partitionName string) " +
          s"stored as " +
          s"parquet location 'file:$testingBaseDirName/testTable'",
          s"alter table $tableName recover partitions")
      )
    }

  }

  describe("stageAndCommitParquetToDB") {

    it("stage csv to parquet and commit to hive") {
      val spark = sparkSession
      val executor = Waimak.sparkExecutor()

      val connector = HiveDummyConnector(SparkFlowContext(spark))
      val connectorRecreate = HiveDummyConnector(SparkFlowContext(spark), forceRecreateTables = true)

      val baseDest = testingBaseDir + "/dest"

      val flow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "items")
        .alias("csv_2", "person")
        .alias("csv_2", "person_recreate")
        .stageAndCommitParquetToDB(connector)(baseDest, partitions = Seq("amount"))("items")
        .stageAndCommitParquetToDB(connector)(baseDest, snapshotFolder = Some("generatedTimestamp=2018-03-13-16-19-00"))("person")
        .stageAndCommitParquetToDB(connectorRecreate)(baseDest, snapshotFolder = Some("generatedTimestamp=2018-03-13-16-19-00"))("person_recreate")

      val (_, finalState) = executor.execute(flow)
      finalState.inputs.size should be(5)

      connector.ranDDLs should be {
        List(List(
          "drop table if exists items",
          s"create external table if not exists items (id integer, item integer) partitioned by (amount string) stored as parquet location 'file:$testingBaseDirName/dest/items'",
          "alter table items recover partitions",
          s"create external table if not exists person (id integer, name string, country string) stored as parquet location 'file:$testingBaseDir/dest/person/generatedTimestamp=2018-03-13-16-19-00'",
          s"alter table person set location 'file:$testingBaseDirName/dest/person/generatedTimestamp=2018-03-13-16-19-00'"
        ))
      }

      connectorRecreate.ranDDLs should be {
        List(List(
          "drop table if exists person_recreate",
          s"create external table if not exists person_recreate (id integer, name string, country string) stored as parquet location 'file:$testingBaseDir/dest/person_recreate/generatedTimestamp=2018-03-13-16-19-00'"
        ))
      }
    }
  }

}