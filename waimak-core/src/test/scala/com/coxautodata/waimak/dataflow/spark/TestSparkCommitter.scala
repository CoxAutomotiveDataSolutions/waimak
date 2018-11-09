package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.{DFExecutorPriorityStrategies, Waimak}

/**
  * Created by Alexei Perelighin on 2018/11/05
  */
class TestSparkCommitter extends SparkAndTmpDirSpec {

  override val appName: String = "Committer tests"

  // Need to explicitly use sequential like executor with preference to loaders
  val executor = Waimak.sparkExecutor(1, DFExecutorPriorityStrategies.preferLoaders)

  import SparkActions._
  import TestSparkData._

  describe("Parquet Commits") {

    describe("no snapshots, no DB commits") {

      it("one table") {
        val spark = sparkSession
        import spark.implicits._

        val baseDest = testingBaseDir + "/dest"

        val flow: SparkDataFlow = Waimak.sparkFlow(spark)
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "purchases")
          .commit("comm_1")("purchases")
          .push("comm_1")(ParquetDataCommitter(baseDest))

        val (executedActions, finalState) = executor.execute(flow)
      }
    }
  }
}
