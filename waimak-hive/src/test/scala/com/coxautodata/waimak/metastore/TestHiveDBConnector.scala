package com.coxautodata.waimak.metastore

import java.io.File

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.TestSparkData._
import com.coxautodata.waimak.dataflow.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException

class TestHiveDBConnector extends SparkAndTmpDirSpec {

  override def builderOptions: SparkSession.Builder => SparkSession.Builder = {
    val build = (sparkSession: SparkSession.Builder) => sparkSession.enableHiveSupport()
      .config("spark.sql.warehouse.dir", s"$basePath/hive")
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:memory:;databaseName=$basePath/derby;create=true")

    super.builderOptions andThen build
  }

  override val appName: String = "Metastore Utils"

  describe("HiveTestConnector") {

    it("should generate a correct drop table schema") {
      val hiveConnection: HadoopDBConnector = HiveDummyConnector(SparkFlowContext(sparkSession))
      hiveConnection.dropTableParquetDDL("testTable") should be("drop table if exists test.testTable")
    }

    it("should generate a correct update table path schema") {
      val hiveConnection: HadoopDBConnector = HiveDummyConnector(SparkFlowContext(sparkSession))
      hiveConnection.updateTableLocationDDL("testTable", "/path") should be("alter table test.testTable set location 'file:/path'")
    }

    it("should generate correct create table statements for non partitioned tables") {
      val hiveConnection: HadoopDBConnector = HiveDummyConnector(SparkFlowContext(sparkSession))
      val tableName = "testTable"
      val testingBaseFile = new File(testingBaseDirName)
      val tablePath = new File(testingBaseFile, tableName)

      sparkSession.read.option("inferSchema", "true").option("header", "true").csv(s"$basePath/csv_1").write.parquet(tablePath.toString)

      //Test non-partition table
      hiveConnection.createTableFromParquetDDL(tableName, tablePath.toURI.getPath) should be(
        List(s"create external table if not exists test.$tableName " +
          "(id int, item int, amount int) stored as " +
          s"parquet location 'file:$testingBaseDirName/testTable'")
      )
    }

    it("should generate correct create table statements for partitioned tables") {
      val hiveConnection: HadoopDBConnector = HiveDummyConnector(SparkFlowContext(sparkSession))
      val tableName = "testTable"
      val partitionName = "amount"
      val testingBaseFile = new File(testingBaseDirName)
      val tablePath = new File(testingBaseFile, tableName)

      sparkSession.read.option("inferSchema", "true").option("header", "true").csv(s"$basePath/csv_1").write.partitionBy(partitionName).parquet(tablePath.toString)

      //Test partitioned table
      hiveConnection.createTableFromParquetDDL(tableName, tablePath.toURI.getPath, partitionColumns = Seq(partitionName)) should be(
        List(s"create external table if not exists test.$tableName " +
          "(id int, item int)" +
          s" partitioned by ($partitionName string) " +
          s"stored as " +
          s"parquet location 'file:$testingBaseDirName/testTable'",
          s"alter table test.$tableName recover partitions")
      )
    }

  }

  describe("stageAndCommitParquetToDB") {

    it("stage csv to parquet and commit to hive") {
      val spark = sparkSession
      val executor = Waimak.sparkExecutor()

      val connector1 = HiveDummyConnector(SparkFlowContext(spark))
      val connector2 = HiveDummyConnector(SparkFlowContext(spark))

      val baseDest = testingBaseDir + "/dest"

      val flow = SparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "items")
        .alias("csv_2", "person")
        .commit("connector1", partitions = Seq("amount"))("items")
        .commit("connector2WithSnapshot")("person")
        .push("connector1")(ParquetDataCommitter(baseDest).withHadoopDBConnector(connector1))
        .push("connector2WithSnapshot")(ParquetDataCommitter(baseDest).withHadoopDBConnector(connector2).withSnapshotFolder("generatedTimestamp=2018-03-13-16-19-00"))

      executor.execute(flow)

      connector1.ranDDLs should be {
        List(List(
          "drop table if exists test.items",
          s"create external table if not exists test.items (id int, item int) partitioned by (amount string) stored as parquet location 'file:$testingBaseDirName/dest/items'",
          "alter table test.items recover partitions"
        ))
      }

      connector2.ranDDLs should be {
        List(List(
          s"create external table if not exists test.person (id int, name string, country string) stored as parquet location 'file:$testingBaseDir/dest/person/generatedTimestamp=2018-03-13-16-19-00'",
          s"alter table test.person set location 'file:$testingBaseDirName/dest/person/generatedTimestamp=2018-03-13-16-19-00'"
        ))
      }

      import HadoopDBConnector._
      spark.conf.set(FORCE_RECREATE_TABLES, true)
      val connectorRecreate = HiveDummyConnector(SparkFlowContext(spark))

      val recreateFlow = SparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_2")
        .alias("csv_2", "person_recreate")
        .commit("connectorRecreateWithSnapshot")("person_recreate")
        .push("connectorRecreateWithSnapshot")(ParquetDataCommitter(baseDest).withHadoopDBConnector(connectorRecreate).withSnapshotFolder("generatedTimestamp=2018-03-13-16-19-00"))

      executor.execute(recreateFlow)

      connectorRecreate.ranDDLs should be {
        List(List(
          "drop table if exists test.person_recreate",
          s"create external table if not exists test.person_recreate (id int, name string, country string) stored as parquet location 'file:$testingBaseDir/dest/person_recreate/generatedTimestamp=2018-03-13-16-19-00'"
        ))
      }
    }
  }

  describe("HiveSparkSQLConnector") {

    it("should create a db for a table if it does not exists with createDatabaseIfNotExists true") {
      val testDb = "test"
      val baseDest = testingBaseDir + "/dest"
      val tableDest = baseDest + "/items"
      val flow = SparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1")
        .alias("csv_1", "items")
        .writeParquet(baseDest)("items")
      val executor = Waimak.sparkExecutor()
      val spark = sparkSession
      import spark.implicits._

      val connector = HiveSparkSQLConnector(SparkFlowContext(sparkSession), testDb, createDatabaseIfNotExists = true)

      spark.sql(s"drop database if exists $testDb cascade")
      spark.sql("show databases").as[String].collect() should be(Seq("default"))

      executor.execute(flow)
      spark.read.parquet(tableDest).as[TPurchase].collect() should be(purchases)

      connector.submitAtomicResultlessQueries(connector.createTableFromParquetDDL("items", tableDest))
      spark.table(s"$testDb.items").as[TPurchase].collect() should be(purchases)

    }

    it("should not create a db for a table if it already exists with createDatabaseIfNotExists true") {
      val testDb = "test"
      val baseDest = testingBaseDir + "/dest"
      val tableDest = baseDest + "/items"
      val flow = SparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1")
        .alias("csv_1", "items")
        .writeParquet(baseDest)("items")
      val executor = Waimak.sparkExecutor()
      val spark = sparkSession
      import spark.implicits._

      val connector = HiveSparkSQLConnector(SparkFlowContext(sparkSession), testDb, createDatabaseIfNotExists = true)

      spark.sql(s"drop database if exists $testDb cascade")
      spark.sql(s"create database $testDb")
      spark.sql("show databases").as[String].collect() should contain theSameElementsAs Seq("default", testDb)

      executor.execute(flow)
      spark.read.parquet(tableDest).as[TPurchase].collect() should be(purchases)

      connector.submitAtomicResultlessQueries(connector.createTableFromParquetDDL("items", tableDest))
      spark.table(s"$testDb.items").as[TPurchase].collect() should be(purchases)

    }

    it("should throw an exception if the database does not exists with createDatabaseIfNotExists false") {
      val testDb = "test"
      val baseDest = testingBaseDir + "/dest"
      val tableDest = baseDest + "/items"
      val flow = SparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1")
        .alias("csv_1", "items")
        .writeParquet(baseDest)("items")
      val executor = Waimak.sparkExecutor()
      val spark = sparkSession
      import spark.implicits._

      val connector = HiveSparkSQLConnector(SparkFlowContext(sparkSession), testDb)

      spark.sql(s"drop database if exists $testDb cascade")
      spark.sql("show databases").as[String].collect() should be(Seq("default"))

      executor.execute(flow)
      spark.read.parquet(tableDest).as[TPurchase].collect() should be(purchases)

      intercept[NoSuchDatabaseException] {
        connector.submitAtomicResultlessQueries(connector.createTableFromParquetDDL("items", tableDest))
      }

    }

    it("should use an existing database for a table if it already exists with createDatabaseIfNotExists false") {
      val testDb = "test"
      val baseDest = testingBaseDir + "/dest"
      val tableDest = baseDest + "/items"
      val flow = SparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1")
        .alias("csv_1", "items")
        .writeParquet(baseDest)("items")
      val executor = Waimak.sparkExecutor()
      val spark = sparkSession
      import spark.implicits._

      val connector = HiveSparkSQLConnector(SparkFlowContext(sparkSession), testDb)

      spark.sql(s"drop database if exists $testDb cascade")
      spark.sql(s"create database $testDb")
      spark.sql("show databases").as[String].collect() should contain theSameElementsAs Seq("default", testDb)

      executor.execute(flow)
      spark.read.parquet(tableDest).as[TPurchase].collect() should be(purchases)

      connector.submitAtomicResultlessQueries(connector.createTableFromParquetDDL("items", tableDest))
      spark.table(s"$testDb.items").as[TPurchase].collect() should be(purchases)

    }

    it("should thrown an exception if  is called") {
      val res = intercept[UnsupportedOperationException] {
        HiveSparkSQLConnector(SparkFlowContext(sparkSession), "").runQueries(Seq.empty)
      }
      res.getMessage should be("HiveSparkSQLConnector does not support running queries that return data. You must use SparkSession.sql directly.")
    }

    it("should handle complex types") {

      val testDb = "test"
      val baseDest = testingBaseDir + "/dest"
      val spark = sparkSession
      import spark.implicits._

      spark.sql(s"drop database if exists $testDb cascade")
      spark.sql(s"create database $testDb")
      spark.sql("show databases").as[String].collect() should contain theSameElementsAs Seq("default", testDb)

      val connector = HiveSparkSQLConnector(SparkFlowContext(sparkSession), testDb)

      val complexTypes = Seq(TComplexTypes(Some(1), Map("a" -> 2, "b" -> 3)))

      val ds = complexTypes.toDS()

      val flow = SparkDataFlow.empty(sparkSession, tmpDir)
        .addInput("complex_types", Some(ds))
        .writeParquet(baseDest)("complex_types")
      val executor = Waimak.sparkExecutor()
      executor.execute(flow)
      connector.submitAtomicResultlessQueries(connector.createTableFromParquetDDL("complex_types", s"$baseDest/complex_types"))
      spark.table(s"$testDb.complex_types").as[TComplexTypes].collect() should be(complexTypes)
    }

  }
}

case class TComplexTypes(id: Option[Int], map: Map[String, Int])