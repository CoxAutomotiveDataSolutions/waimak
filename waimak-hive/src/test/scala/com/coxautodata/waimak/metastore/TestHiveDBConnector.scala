package com.coxautodata.waimak.metastore

import java.io.File

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.TestSparkData._
import com.coxautodata.waimak.dataflow.spark._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}

class TestHiveDBConnector extends SparkAndTmpDirSpec {

  override def builderOptions: SparkSession.Builder => SparkSession.Builder = {
    _.enableHiveSupport()
      .config("spark.sql.warehouse.dir", s"$basePath/hive")
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:memory:;databaseName=$basePath/derby;create=true")
  }

  override val appName: String = "Metastore Utils"

  describe("HiveTestConnector") {

    it("should generate a correct drop table schema") {
      val hiveConnection: HadoopDBConnector = HiveDummyConnector(SparkFlowContext(sparkSession))
      hiveConnection.dropTableParquetDDL("testTable") should be("drop table if exists testTable")
    }

    it("should generate a correct update table path schema") {
      val hiveConnection: HadoopDBConnector = HiveDummyConnector(SparkFlowContext(sparkSession))
      hiveConnection.updateTableLocationDDL("testTable", "/path") should be("alter table testTable set location 'file:/path'")
    }

    it("should generate correct create table statements for non partitioned tables") {
      val hiveConnection: HadoopDBConnector = HiveDummyConnector(SparkFlowContext(sparkSession))
      val tableName = "testTable"
      val testingBaseFile = new File(testingBaseDirName)
      val tablePath = new File(testingBaseFile, tableName)

      sparkSession.read.option("inferSchema", "true").option("header", "true").csv(s"$basePath/csv_1").write.parquet(tablePath.toString)

      //Test non-partition table
      hiveConnection.createTableFromParquetDDL(tableName, tablePath.toURI.getPath) should be(
        List(s"create external table if not exists $tableName " +
          "(id integer, item integer, amount integer) stored as " +
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

      val connector1 = HiveDummyConnector(SparkFlowContext(spark))
      val connector2 = HiveDummyConnector(SparkFlowContext(spark))
      val connectorRecreate = HiveDummyConnector(SparkFlowContext(spark))

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

      connector1.ranDDLs should be {
        List(List(
          "drop table if exists items",
          s"create external table if not exists items (id integer, item integer) partitioned by (amount string) stored as parquet location 'file:$testingBaseDirName/dest/items'",
          "alter table items recover partitions"
        ))
      }

      connector2.ranDDLs should be {
        List(List(
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

    it("getPathsAndPartitionsForTables") {
      val testDb = "test"
      val baseDest = testingBaseDir + "/dest"
      val connector = HiveSparkSQLConnector(SparkFlowContext(sparkSession), testDb, createDatabaseIfNotExists = true)
      connector.submitAtomicResultlessQueries(Seq(s"use $testDb"))
      val spark = sparkSession

      val df = spark.read.option("header", "true").option("inferSchema", "true").csv(s"$basePath/csv_1")

      df.write.partitionBy("amount").parquet(s"$baseDest/partitionedBy1")
      df.write.partitionBy("amount", "item").parquet(s"$baseDest/partitionedBy2")
      df.write.parquet(s"$baseDest/noPartitions")

      connector.submitAtomicResultlessQueries(Seq(
        connector.recreateTableFromParquetDDLs("partitionedBy1", s"$baseDest/partitionedBy1", Seq("amount")),
        connector.recreateTableFromParquetDDLs("partitionedBy2", s"$baseDest/partitionedBy2", Seq("amount", "item")),
        connector.recreateTableFromParquetDDLs("noPartitions", s"$baseDest/noPartitions")
      ).flatten)

      val res = connector.getPathsAndPartitionsForTables(Seq("partitionedBy1", "partitionedBy2", "noPartitions", "noTable"))

      res should contain theSameElementsAs Map(
        "partitionedBy1" -> TablePathAndPartitions(Some(new Path(s"file:$baseDest/partitionedBy1")), Seq("amount")),
        "partitionedBy2" -> TablePathAndPartitions(Some(new Path(s"file:$baseDest/partitionedBy2")), Seq("amount", "item")),
        "noPartitions" -> TablePathAndPartitions(Some(new Path(s"file:$baseDest/noPartitions")), Seq.empty),
        "noTable" -> TablePathAndPartitions(None, Seq.empty)
      )

    }

    it("end-to-end with a new table"){
      val testDb = "test"
      val testTable = "test"
      val baseDest = testingBaseDir + "/dest"
      val baseTemp = testingBaseDir + "/temp"
      val connector = HiveSparkSQLConnector(SparkFlowContext(sparkSession), testDb, createDatabaseIfNotExists = true)
      connector.submitAtomicResultlessQueries(Seq(s"use $testDb"))
      val spark = sparkSession
      import spark.implicits._
      val df = spark.read.option("header", "true").option("inferSchema", "true").csv(s"$basePath/csv_1")

      intercept[NoSuchTableException]{
        spark.sql(s"show create table $testTable")
      }

      val flow = Waimak
        .sparkFlow(spark, baseTemp)
        .addInput(testTable, Some(df))
        .commit("commit")(testTable)
        .push("commit")(ParquetDataCommitter(baseDest).withSnapshotFolder("snap=2").withHadoopDBConnector(connector))

      val (_, resFlow) = Waimak.sparkExecutor().execute(flow)

      spark.sql(s"show create table $testTable").as[String].collect().head.lines.takeWhile(!_.startsWith("TBLPROPERTIES")).mkString("\n") should be (
        s"""CREATE EXTERNAL TABLE `$testTable`(`id` int, `item` int, `amount` int)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          |WITH SERDEPROPERTIES (
          |  'serialization.format' = '1'
          |)
          |STORED AS
          |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
          |LOCATION 'file:$baseDest/$testTable/snap=2'""".stripMargin
      )
      resFlow.inputs.get[TablePathAndPartitions](s"${testTable}_CURRENT_TABLE_PATH_AND_PARTITIONS") should be (TablePathAndPartitions(None, Seq.empty))
      resFlow.inputs.get[TablePathAndPartitions](s"${testTable}_NEW_TABLE_PATH_AND_PARTITIONS") should be (TablePathAndPartitions(Some(new Path(s"file:$baseDest/$testTable/snap=2")), Seq.empty))
      resFlow.inputs.get[Boolean](s"${testTable}_SCHEMA_CHANGED") should be (true)

    }

  }

}