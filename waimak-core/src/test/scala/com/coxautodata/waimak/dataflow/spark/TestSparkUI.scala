package org.apache.spark

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.TestSparkData.basePath
import com.coxautodata.waimak.dataflow.spark.{SimpleSparkDataFlow, SparkAndTmpDirSpec, TestTwoInputsAndOutputsAction}
import org.apache.spark.sql.functions.{count, lit, sum}
import org.apache.spark.ui.WaimakExecutionsUITab

class TestSparkUI extends SparkAndTmpDirSpec {
  override val appName: String = "Spark UI"

  override val sparkOptions: Map[String, String] = Map("spark.executor.memory" -> "2g", "spark.ui.enabled" -> "true")

  val executor = Waimak.sparkExecutor()

  describe("spark ui") {

/*    it("create tab") {
      val spark = sparkSession

      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1", "csv_2")
        .show("csv_1")
        .addAction(new TestTwoInputsAndOutputsAction(List("csv_1", "csv_2"), List("csv_1_a", "csv_2_a"), (a, b) => ( {
          a.count;
          a
        }, {
          b.count;
          b
        })))

      val (executedActions, finalState) = executor.execute(flow)
      //Thread.sleep(1000000000L)
      Thread.sleep(30000L) // 30secs
    }
    */
/*
    it("create tab 2") {
      val spark = sparkSession
      val flow1 = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1", "csv_2")
        //.openCSV(basePath)("csv_2", "csv_1")
        .show("csv_1")
        .show("csv_2")
        .show("csv_1")
        .addAction(new TestTwoInputsAndOutputsAction(List("csv_1", "csv_2"), List("csv_1_a", "csv_2_a"), (a, b) => ( {
          a.count; a
        }, {
          b.count; b
        })))

      val (executedActions1, finalState1) = executor.execute(flow1)
      //Thread.sleep(1000000000L)
      //Thread.sleep(300000L)// 5mins
      //Thread.sleep(120000L) // 2mins
      //Thread.sleep(60000L) // 1min
      Thread.sleep(30000L) // 30secs
    }*/

/*    it("tag dependency between write and open") {
      // This will fix the missing file error by providing a dependency using tags
      val spark = sparkSession
      import spark.implicits._
      val baseDest = testingBaseDir + "/dest"

      val flow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        .tagDependency("written") {
          _.openFileParquet(s"$baseDest/person", "person_written")
            .openFileParquet(s"$baseDest/items", "items_written")
        }
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "items")
        .alias("csv_2", "person")
        .tag("written") {
          _.writeParquet(baseDest)("person", "items")
        }

      val (executedActions, finalState) = executor.execute(flow)
      Thread.sleep(30000L) // 30secs
    }*/

/*    it("stage and commit parquet, and force a cache as parquet") {
      val spark = sparkSession
      //import spark.implicits._
      val baseDest = testingBaseDir + "/dest"
      val flow = Waimak.sparkFlow(spark, s"$baseDest/tmp")
        .openCSV(basePath)("csv_1")
        .alias("csv_1", "parquet_1")
        .cacheAsParquet("parquet_1")
        .inPlaceTransform("parquet_1")(df => df)
        .inPlaceTransform("parquet_1")(df => df)
        .stageAndCommitParquet(baseDest, snapshotFolder = Some("generated_timestamp=20180509094500"))("parquet_1")

      val (executedActions, finalState) = executor.execute(flow)
      Thread.sleep(30000L) // 30secs
    }*/

/*
    it("chain one by one") {
      val spark = sparkSession
      import spark.implicits._
      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1", "csv_2")
        .transform("csv_1")("person_summary") { csv_1 =>
          csv_1.groupBy($"id").agg(count($"id").as("item_cnt"), sum($"amount").as("total"))
        }.transform("person_summary", "csv_2")("report") { (person_summary, csv_2) =>
        csv_2.join(person_summary, csv_2("id") === person_summary("id"), "left")
          .drop(person_summary("id"))
          .withColumn("calc_1", lit(2))
      }.printSchema("report")
        .show("report")

      val (executedActions, finalState) = executor.execute(flow)
      Thread.sleep(30000L) // 30secs
    }
*/

    it("stage csv to parquet and commit then use label afterwards") {
      val spark = sparkSession
      val baseDest = testingBaseDir + "/dest"

      val flow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_2")
        .alias("csv_2", "person")
        .stageAndCommitParquet(baseDest, snapshotFolder = Some("generatedTimestamp=2018-03-13-16-19-00"))("person")
        .transform("person")("person_1") {
          df =>
            df.sparkSession.catalog.clearCache()
            df
        }
        .show("person_1")

      val (executedActions, finalState) = executor.execute(flow)
      Thread.sleep(30000L) // 30secs
    }

  }
}