package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import org.apache.hadoop.fs.Path

/**
  * Created by Alexei Perelighin on 2018/02/28
  */
class TestSparkInterceptors extends SparkAndTmpDirSpec {

  override val appName: String = "Spark Interceptors"

  val executor = Waimak.sparkExecutor()

  import TestSparkData._

  describe("post") {

    it("project output") {
      val flow = Waimak.sparkFlow(sparkSession)
        .openCSV(basePath)("csv_1")
        .show("csv_1")
        .printSchema("csv_1")
        .debugAsTable("csv_1")

      val actionNum = flow.actions.size

      val withIntercept = flow.inPlaceTransform("csv_1") { c => c.select("id") }

      withIntercept.actions.size should be(actionNum)

      val (executedActions, finalState) = executor.execute(withIntercept)

      val d = sparkSession.sql("select * from csv_1")
      d.schema.fieldNames should be(Array("id"))
    }

    it("snapshot") {
      val spark = sparkSession
      import spark.implicits._

      val flow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .alias("csv_1", "purchases")
        .show("purchases")
        .printSchema("purchases")
        .debugAsTable("purchases")

      val actionNum = flow.actions.size

      val withIntercept = flow.cacheAsParquet("purchases")

      withIntercept.actions.size should be(actionNum)

      val (executedActions, finalState) = executor.execute(withIntercept)

      val readBack = sparkSession.read.parquet(new Path(tmpDir, "purchases").toString)
      readBack.show()
      readBack.as[TPurchase].collect() should be(purchases)
    }

    it("combine post and snapshot") {
      val spark = sparkSession
      import spark.implicits._

      val flow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .alias("csv_1", "purchases")
        .show("purchases")
        .printSchema("purchases")
        .debugAsTable("purchases")
        .inPlaceTransform("purchases"){d => print("purchases " + d.schema.prettyJson); d}

      val actionNum = flow.actions.size

      val withIntercept = flow
        .inPlaceTransform("purchases") {
          _.select("id").distinct.orderBy("id")
        }
        .cacheAsParquet("purchases")

      withIntercept.actions.size should be(actionNum)

      val (executedActions, finalState) = executor.execute(withIntercept)

      finalState.inputs.size should be(2)
      finalState.inputs.get("csv_1").map(_.as[TPurchase].collect()).get should be(purchases)

      val readBack = sparkSession.read.parquet(new Path(tmpDir, "purchases").toString)
      readBack.show()
      readBack.as[Int].collect().sorted should be(Array(1, 2, 3, 5))

    }

    it("sql with interceptor action") {
      val spark = sparkSession
      import spark.implicits._

      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1")
        .sql("csv_1")("person_summary", "select id, count(item) as item_cnt, sum(amount) as total from csv_1 group by id")
        .inPlaceTransform("person_summary")(_.limit(1))
        .sql("person_summary")("person_summary_count", "select count(*) from person_summary")
        .show("person_summary_count")

      val (executedActions, finalState) = executor.execute(flow)

      finalState.actions.size should be(0) // no actions to execute
      finalState.inputs.size should be(3)
      finalState.inputs.get("person_summary_count").map(_.as[Long].collect()).get should be(Array(1))
    }
  }
}
