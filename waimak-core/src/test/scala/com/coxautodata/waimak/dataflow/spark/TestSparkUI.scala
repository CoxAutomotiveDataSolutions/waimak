package org.apache.spark

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.TestSparkData.basePath
import com.coxautodata.waimak.dataflow.spark.{SparkAndTmpDirSpec, TestTwoInputsAndOutputsAction}
import org.apache.spark.ui.WaimakExecutionsUITab

class TestSparkUI extends SparkAndTmpDirSpec {
  override val appName: String = "Spark UI"

  override val sparkOptions: Map[String, String] = Map("spark.executor.memory" -> "2g", "spark.ui.enabled" -> "true")

  val executor = Waimak.sparkExecutor()

  describe("spark ui") {

    it("create tab") {
      val spark = sparkSession

      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1", "csv_2")
        .show("csv_1")
        .addAction(new TestTwoInputsAndOutputsAction(List("csv_1", "csv_2"), List("csv_1_a", "csv_2_a"), (a, b) => ( {
          a.count; a
        }, {
          b.count; b
        })))

      val (executedActions, finalState) = executor.execute(flow)
      Thread.sleep(1000000000L)

    }
  }
}