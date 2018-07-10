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

    /*
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

          */

/*

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
    }

*/
    it("tag dependency between write and open") {
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
      //val (_, finalState) = sequentialExecutor.execute(flow)
      //finalState.inputs.size should be(6)

      //finalState.inputs.get("items_written").get.as[TPurchase].collect() should be(purchases)
      //finalState.inputs.get("person_written").get.as[TPerson].collect() should be(persons)

    //case class TPurchase(id: Option[Int], item: Option[Int], amount: Option[Int])

    //case class TPerson(id: Option[Int], name: Option[String], country: Option[String])
/*    it("create tab 2") {
      val spark = sparkSession*/
    }
  }
}