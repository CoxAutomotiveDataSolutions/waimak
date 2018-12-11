package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.{DFExecutorPriorityStrategies, DataFlowException, Waimak}
import org.apache.hadoop.fs.Path

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

        val flow: SparkDataFlow = Waimak.sparkFlow(spark, tmpDir.toString)
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "purchases")
          .commit("comm_1")("purchases")
          .push("comm_1")(ParquetDataCommitter(baseDest))

        flow.flowContext.fileSystem.exists(new Path(baseDest)) should be(false)

        executor.execute(flow)

        spark.read.parquet(baseDest + "/purchases").as[TPurchase].collect() should be(purchases)
      }

    }

    describe("failures") {

      it("no commit for a push") {
        val baseDest = testingBaseDir + "/dest"

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "purchases")
          .commit("comm_1")("purchases")
          .push("comm_1")(ParquetDataCommitter(baseDest))
          .push("no_commit_1")(ParquetDataCommitter(baseDest))
          .push("no_commit_2")(ParquetDataCommitter(baseDest))

        flow.flowContext.fileSystem.exists(new Path(baseDest)) should be(false)

        val res = intercept[DataFlowException] {
          executor.execute(flow)
        }
        res.text should be("There are no commits definitions for pushes: [no_commit_1, no_commit_2]")
      }

      it("no push for a commit") {
        val baseDest = testingBaseDir + "/dest"

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .openCSV(basePath)("csv_1", "csv_2")
          .alias("csv_1", "purchases")
          .commit("comm_1")("purchases")
          .push("comm_1")(ParquetDataCommitter(baseDest))
          .commit("no_push_1")("csv_2")

        flow.flowContext.fileSystem.exists(new Path(baseDest)) should be(false)

        val res = intercept[DataFlowException] {
          executor.execute(flow)
        }
        res.text should be("There are no push definitions for commits: [no_push_1]")
      }

      it("same label in 2 commits") {
        val spark = sparkSession
        import spark.implicits._

        val baseDestCommit1 = testingBaseDir + "/comm_1"
        val baseDestCommit2 = testingBaseDir + "/with_duplicate"

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .openCSV(basePath)("csv_1", "csv_2")
          .alias("csv_1", "purchases")
          .commit("comm_1")("purchases", "csv_2")
          .commit("with_duplicate")("csv_2")
          .push("comm_1")(ParquetDataCommitter(baseDestCommit1))
          .push("with_duplicate")(ParquetDataCommitter(baseDestCommit2))

        flow.flowContext.fileSystem.exists(new Path(baseDestCommit1)) should be(false)
        flow.flowContext.fileSystem.exists(new Path(baseDestCommit2)) should be(false)

        executor.execute(flow)

        spark.read.parquet(baseDestCommit1 + "/csv_2").as[TPerson].collect() should be(persons)
        spark.read.parquet(baseDestCommit2 + "/csv_2").as[TPerson].collect() should be(persons)
      }

      it("commit label used in an input of an action") {
        val spark = sparkSession
        import spark.implicits._

        val baseCommitDest = testingBaseDir + "/commit"
        val baseWriteDest = testingBaseDir + "/write"

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .openCSV(basePath)("csv_1", "csv_2")
          .writeParquet(baseWriteDest)("csv_1")
          .commit("comm_1")("csv_1", "csv_2")
          .push("comm_1")(ParquetDataCommitter(baseCommitDest))

        executor.execute(flow)

        spark.read.parquet(baseCommitDest + "/csv_1").as[TPurchase].collect() should be(purchases)
        spark.read.parquet(baseWriteDest + "/csv_1").as[TPurchase].collect() should be(purchases)

      }

      it("commit label also cached and used as the input of another action") {
        val spark = sparkSession
        import spark.implicits._

        val baseCommitDest = testingBaseDir + "/commit"
        val baseWriteDest = testingBaseDir + "/write"

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .openCSV(basePath)("csv_1", "csv_2")
          .cacheAsParquet("csv_1")
          .writeParquet(baseWriteDest)("csv_1")
          .commit("comm_1")("csv_1", "csv_2")
          .push("comm_1")(ParquetDataCommitter(baseCommitDest))

        executor.execute(flow)

        spark.read.parquet(baseCommitDest + "/csv_1").as[TPurchase].collect() should be(purchases)
        spark.read.parquet(baseWriteDest + "/csv_1").as[TPurchase].collect() should be(purchases)

      }

      it("commit label also cached and used as the input of another action with the commit name the same as the label") {
        val spark = sparkSession
        import spark.implicits._

        val baseCommitDest = testingBaseDir + "/commit"
        val baseWriteDest = testingBaseDir + "/write"

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .openCSV(basePath)("csv_1")
          .cacheAsParquet("csv_1")
          .writeParquet(baseWriteDest)("csv_1")
          .commit("csv_1")("csv_1")
          .push("csv_1")(ParquetDataCommitter(baseCommitDest))

        executor.execute(flow)

        spark.read.parquet(baseCommitDest + "/csv_1").as[TPurchase].collect() should be(purchases)
        spark.read.parquet(baseWriteDest + "/csv_1").as[TPurchase].collect() should be(purchases)

      }

      it("commit label is not produced by any action action") {
        val baseDest = testingBaseDir + "/dest"

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "purchases")
          .commit("comm_1")("csv_1", "csv_2")
          .push("comm_1")(ParquetDataCommitter(baseDest))

        flow.flowContext.fileSystem.exists(new Path(baseDest)) should be(false)

        val res = intercept[DataFlowException] {
          executor.execute(flow)
        }
        res.text should be("Commit definitions with labels that are not produced by any action: [comm_1 -> {csv_2}]")
      }
    }

    describe("success") {

      it("add label to existing commit") {
        val spark = sparkSession
        import spark.implicits._

        val baseDest = testingBaseDir + "/dest"

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .openCSV(basePath)("csv_1", "csv_2")
          .alias("csv_1", "purchases")
          .commit("comm_1")("purchases")
          .commit("comm_1")("csv_2")
          .push("comm_1")(ParquetDataCommitter(baseDest))

        flow.flowContext.fileSystem.exists(new Path(baseDest)) should be(false)

        executor.execute(flow)

        spark.read.parquet(baseDest + "/purchases").as[TPurchase].collect() should be(purchases)
        spark.read.parquet(baseDest + "/csv_2").as[TPerson].collect() should be(persons)
      }

      it("commit pre aliased label") {
        val spark = sparkSession
        import org.apache.spark.sql.functions._
        import spark.implicits._

        val baseDest = testingBaseDir + "/dest"

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .openCSV(basePath)("csv_1", "csv_2")
          .alias("csv_1", "purchases")
          .alias("csv_2", "buyers")
          .transform("purchases", "buyers")("report") {
            (p, b) =>
              val personSummary = p.groupBy('id).agg('id, count('item).as("item_cnt"), sum('amount).as("total"))
              b.join(personSummary, Seq("id"), "left").withColumn("calc_1", lit(2))
          }
          .commit("comm_1")("purchases", "report")
          .push("comm_1")(ParquetDataCommitter(baseDest))

        flow.flowContext.fileSystem.exists(new Path(baseDest)) should be(false)

        executor.execute(flow)

        spark.read.parquet(baseDest + "/purchases").as[TPurchase].collect() should be(purchases)
        spark.read.parquet(baseDest + "/report").as[TReport].collect() should be(report)
      }

      describe("with snapshots, no DB commits") {

        it("one table, single snapshot, no cleaning") {
          val spark = sparkSession
          import spark.implicits._

          val baseDest = testingBaseDir + "/dest"

          val flow: SparkDataFlow = Waimak.sparkFlow(spark, tmpDir.toString)
            .openCSV(basePath)("csv_1")
            .alias("csv_1", "purchases")
            .commit("comm_1")("purchases")
            .push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot_1"))

          flow.flowContext.fileSystem.exists(new Path(baseDest)) should be(false)

          val (executedActions, finalState) = executor.execute(flow)

          spark.read.parquet(baseDest + "/purchases/snapshot_1").as[TPurchase].collect() should be(purchases)
        }

        it("one table, multiple snapshots, no cleaning") {
          val spark = sparkSession
          import spark.implicits._

          val baseDest = testingBaseDir + "/dest"

          val flowPrePush: SparkDataFlow = Waimak.sparkFlow(spark, tmpDir.toString)
            .openCSV(basePath)("csv_1")
            .alias("csv_1", "purchases")
            .commit("comm_1")("purchases")

          flowPrePush.flowContext.fileSystem.exists(new Path(baseDest)) should be(false)

          val flow_1 = flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot_1"))
          executor.execute(flow_1)

          val flow_2 = flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot_2"))
          executor.execute(flow_2)

          spark.read.parquet(baseDest + "/purchases/snapshot_1").as[TPurchase].collect() should be(purchases)
          spark.read.parquet(baseDest + "/purchases/snapshot_2").as[TPurchase].collect() should be(purchases)
        }

        it("one table, multiple snapshots, with cleaning") {
          val spark = sparkSession
          import spark.implicits._

          val baseDest = testingBaseDir + "/dest"

          val flowPrePush: SparkDataFlow = Waimak.sparkFlow(spark, tmpDir.toString)
            .openCSV(basePath)("csv_1")
            .alias("csv_1", "purchases")
            .commit("comm_1")("purchases")

          flowPrePush.flowContext.fileSystem.exists(new Path(baseDest)) should be(false)

          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181101_123001_567")))
          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181101_123001_568")))
          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181103_123001_567")))
          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181103_123001_568")))
          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181105_123001_567")))
          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181105_123001_568")))

          spark.read.parquet(baseDest + "/purchases/snapshot=20181101_123001_567").as[TPurchase].collect() should be(purchases)
          spark.read.parquet(baseDest + "/purchases/snapshot=20181105_123001_568").as[TPurchase].collect() should be(purchases)

          executor.execute(flowPrePush.push("comm_1")(
            ParquetDataCommitter(baseDest)
              .snapshotFolder("snapshot=20181105_123001_569")
              .dateBasedSnapshotCleanup("snapshot", "yyyyMMdd_HHmmss_SSS", 3))
          )

          flowPrePush.flowContext.fileSystem.listStatus(new Path(baseDest + "/purchases")).map(_.getPath.getName).sorted should be(Array("snapshot=20181105_123001_567", "snapshot=20181105_123001_568", "snapshot=20181105_123001_569"))
        }

        it("multiple tables, multiple snapshots, with cleaning") {
          val spark = sparkSession
          import spark.implicits._

          val baseDest = testingBaseDir + "/dest"

          val flowPrePush: SparkDataFlow = Waimak.sparkFlow(spark, tmpDir.toString)
            .openCSV(basePath)("csv_1", "csv_2")
            .alias("csv_1", "purchases")
            .alias("csv_2", "buyers")
            .commit("comm_1")("purchases", "buyers")

          flowPrePush.flowContext.fileSystem.exists(new Path(baseDest)) should be(false)

          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181101_123001_567")))
          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181101_123001_568")))
          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181103_123001_567")))
          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181103_123001_568")))
          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181105_123001_567")))
          executor.execute(flowPrePush.push("comm_1")(ParquetDataCommitter(baseDest).snapshotFolder("snapshot=20181105_123001_568")))

          spark.read.parquet(baseDest + "/purchases/snapshot=20181101_123001_567").as[TPurchase].collect() should be(purchases)
          spark.read.parquet(baseDest + "/purchases/snapshot=20181105_123001_568").as[TPurchase].collect() should be(purchases)
          spark.read.parquet(baseDest + "/buyers/snapshot=20181101_123001_567").as[TPerson].collect() should be(persons)
          spark.read.parquet(baseDest + "/buyers/snapshot=20181105_123001_568").as[TPerson].collect() should be(persons)

          executor.execute(flowPrePush.push("comm_1")(
            ParquetDataCommitter(baseDest)
              .snapshotFolder("snapshot=20181105_123001_569")
              .dateBasedSnapshotCleanup("snapshot", "yyyyMMdd_HHmmss_SSS", 3))
          )

          flowPrePush.flowContext.fileSystem.listStatus(new Path(baseDest + "/purchases")).map(_.getPath.getName).sorted should be(Array("snapshot=20181105_123001_567", "snapshot=20181105_123001_568", "snapshot=20181105_123001_569"))
          flowPrePush.flowContext.fileSystem.listStatus(new Path(baseDest + "/buyers")).map(_.getPath.getName).sorted should be(Array("snapshot=20181105_123001_567", "snapshot=20181105_123001_568", "snapshot=20181105_123001_569"))
        }

      }

    }
  }
}
