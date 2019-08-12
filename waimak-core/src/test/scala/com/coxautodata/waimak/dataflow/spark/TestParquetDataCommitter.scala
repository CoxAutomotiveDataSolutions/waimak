package com.coxautodata.waimak.dataflow.spark

import java.io.File

import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.dataflow.DataFlow._
import com.coxautodata.waimak.dataflow.spark.ParquetDataCommitter._
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.TestSparkData.{basePath, purchases}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset
import com.coxautodata.waimak.filesystem.FSUtils._

import scala.util.{Failure, Success}

/**
  * Created by Alexei Perelighin on 2018/11/28
  */
class TestParquetDataCommitter extends SparkAndTmpDirSpec {

  override val appName: String = "Parquet Committer tests"

  val dateFormat = "yyyyMMdd_HHmmss_SSS"

  val snapshotFoldersDifferentDays = Seq(
    "snapshotFolder=20181101_123001_567"
    , "snapshotFolder=20181102_123001_567"
    , "snapshotFolder=20181103_123001_567"
    , "snapshotFolder=20181104_123001_567"
    , "snapshotFolder=20181105_123001_567"
    , "snapshotFolder=20181106_123001_567"
    , "snapshotFolder=20181107_123001_567"
    , "snapshotFolder=20181108_123001_567"
    , "snapshotFolder=20181109_123001_567"
    , "snapshotFolder=20181110_123001_567"
    , "snapshotFolder=20181111_123001_567"
  )

  val snapshotFoldersSameDay = Seq(
    "snapshotFolder=20181101_123001_567"
    , "snapshotFolder=20181101_123001_568"
    , "snapshotFolder=20181101_123001_569"
    , "snapshotFolder=20181101_123003_567"
    , "snapshotFolder=20181101_123041_567"
    , "snapshotFolder=20181101_123041_568"
    , "snapshotFolder=20181101_123041_569"
  )

  def initSnapshotFolders(basePath: File, snapshotFolders: Seq[String]): Unit = {
    snapshotFolders.map(new File(basePath, _)).foreach(_.mkdirs())
  }

  describe("date based cleanup") {

    val strategyToRemove = dateBasedSnapshotCleanupStrategy[String]("snapshotFolder", dateFormat, 5)(identity)

    it("empty") {
      strategyToRemove("table_1", Seq.empty) should be(Seq.empty[String])
    }

    it("one folder") {
      strategyToRemove("table_1", snapshotFoldersDifferentDays.take(1)) should be(Seq.empty[String])
    }

    it("exact as numberOfFoldersToKeep") {
      strategyToRemove("table_1", snapshotFoldersDifferentDays.take(5)) should be(Seq.empty[String])
    }

    it("exact as numberOfFoldersToKeep + 1 and input is sorted") {
      strategyToRemove("table_1", snapshotFoldersDifferentDays.take(6)) should be(Seq("snapshotFolder=20181101_123001_567"))
    }

    it("exact as numberOfFoldersToKeep + 1 and input is in reverse") {
      strategyToRemove("table_1", snapshotFoldersDifferentDays.take(6).reverse) should be(Seq("snapshotFolder=20181101_123001_567"))
    }

    it("all in reverse") {
      strategyToRemove("table_1", snapshotFoldersDifferentDays.reverse) should be(snapshotFoldersDifferentDays.take(6).reverse)
    }

    it("all, with non complient name of files and folders") {
      strategyToRemove("table_1", (snapshotFoldersDifferentDays ++ Seq("_SUCCESS", "odd=file", "snapshotFolder", "snapshotFolder_1=20181101_123000_567")).reverse) should be(snapshotFoldersDifferentDays.take(6).reverse)
    }

    it("same day") {
      strategyToRemove("table_1", snapshotFoldersSameDay.reverse) should be(Seq("snapshotFolder=20181101_123001_568", "snapshotFolder=20181101_123001_567"))
    }
  }

  describe("validations") {

    describe("failures") {

      it("no temp folder") {
        val baseDest = testingBaseDir + "/dest"
        val committer = ParquetDataCommitter(baseDest)
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession)
        val res = committer.validate(flow, "f1", Seq.empty)
        res shouldBe a[Failure[_]]
        res.failed.get.getMessage should be("ParquetDataCommitter [f1], temp folder is not defined")
      }

      it("cleanup without snapshot folder") {
        val baseDest = testingBaseDir + "/dest"
        val committer = ParquetDataCommitter(baseDest).withDateBasedSnapshotCleanup("snap", "YYYY", 1)
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        val res = committer.validate(flow, "f1", Seq.empty)
        res shouldBe a[Failure[_]]
        res.failed.get.getMessage should be("ParquetDataCommitter [f1], cleanup will only work when snapshot folder is defined")
      }

      it("snapshot folder already exists for 2 labels out of 3") {
        val baseDest = testingBaseDir + "/dest"
        val committer = ParquetDataCommitter(baseDest)
          .withSnapshotFolder("snap=2001")
          .withDateBasedSnapshotCleanup("snap", "YYYY", 1)
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)

        initSnapshotFolders(new File(baseDest, "label_fail_1"), Seq("snap=2000", "snap=2001"))
        initSnapshotFolders(new File(baseDest, "label_fail_2"), Seq("snap=2000", "snap=2001"))
        initSnapshotFolders(new File(baseDest, "label_ok"), Seq("snap=2000"))

        val res = committer.validate(flow, "f1", Seq(
          CommitEntry("label_fail_1", "f1", None, repartition = false, cache = true)
          , CommitEntry("label_fail_2", "f1", None, repartition = false, cache = true)
          , CommitEntry("label_ok", "f1", None, repartition = false, cache = true))
        )
        res shouldBe a[Failure[_]]
        res.failed.get.getMessage should be("ParquetDataCommitter [f1], snapshot folder [snap=2001] is already present for labels: [label_fail_1, label_fail_2]")
      }
    }

    describe("success") {

      import CommitMetadataExtension._

      it("bare minimum") {
        val baseDest = testingBaseDir + "/dest"
        val committer = ParquetDataCommitter(baseDest)
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        committer.validate(flow, "fff", Seq(CommitEntry("label_1", "fff", None, repartition = false, cache = true))) should be(Success())
      }

      it("with snapshot and no cleanup") {
        val baseDest = testingBaseDir + "/dest"
        val committer = ParquetDataCommitter(baseDest).withSnapshotFolder("ts=777777")
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        committer.validate(flow, "fff", Seq(CommitEntry("label_1", "fff", None, repartition = false, cache = true))) should be(Success())
      }

      it("multiple labels, no clash of the snapshot folders of committed labels") {
        val baseDest = testingBaseDir + "/dest"
        val committer = ParquetDataCommitter(baseDest)
          .withSnapshotFolder("snap=2003")
          .withDateBasedSnapshotCleanup("snap", "YYYY", 1)
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)

        initSnapshotFolders(new File(baseDest, "label_not_committed"), Seq("snap=2000", "snap=2003"))
        initSnapshotFolders(new File(baseDest, "label_1"), Seq("snap=2000", "snap=2001"))
        initSnapshotFolders(new File(baseDest, "label_2"), Seq("snap=2000", "snap=2001"))
        initSnapshotFolders(new File(baseDest, "label_3"), Seq("snap=2000"))

        val res = committer.validate(flow, "f1", Seq(
          CommitEntry("label_1", "f1", None, repartition = false, cache = true)
          , CommitEntry("label_2", "f1", None, repartition = false, cache = true)
          , CommitEntry("label_3", "f1", None, repartition = false, cache = true)
          , CommitEntry("label_no_init", "f1", None, repartition = false, cache = true))
        )
        res should be(Success())
      }

      it("commit a parquet and make sure one label is cached if it is used as input elsewhere") {
        val spark = sparkSession
        import spark.implicits._

        val baseDest = testingBaseDir + "/dest"

        val flow = Waimak.sparkFlow(spark, s"$baseDest/tmp")
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "items")
          .commit("commit")("items")
          .push("commit")(ParquetDataCommitter(baseDest).withSnapshotFolder("generated_timestamp=20180509094500"))
          .alias("items", "items_aliased")

        val (ex, _) = Waimak.sparkExecutor().execute(flow)

        // Check to see if the alias action has been intercepted
        val interceptorAction = ex.filter(_.outputLabels.contains("items")).head
        interceptorAction shouldBe a[PostActionInterceptor[_]]

        // Check the post action is a cache
        val typedInterceptorAction = interceptorAction.asInstanceOf[PostActionInterceptor[Dataset[_]]]
        typedInterceptorAction.postActions.length should be(1)
        typedInterceptorAction.postActions.head shouldBe a[CachePostAction[_]]

        spark.read.parquet(s"$baseDest/items").as[TPurchase].collect() should be(purchases)
      }

      it("commit a repartitioned parquet and make sure one label is cached if it is used as input elsewhere") {
        val spark = sparkSession
        import spark.implicits._
        spark.conf.set(SparkDataFlow.REMOVE_TEMP_AFTER_EXECUTION, false)

        val baseDest = testingBaseDir + "/dest"

        val flow = Waimak.sparkFlow(spark, s"$baseDest/tmp")
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "items")
          .commit("commit", 3)("items")
          .push("commit")(ParquetDataCommitter(baseDest))
          .alias("items", "items_aliased")

        val (ex, _) = Waimak.sparkExecutor().execute(flow)

        spark.read.parquet(s"$baseDest/items").as[TPurchase].collect() should contain theSameElementsAs purchases

        // Check repartition count
        flow.flowContext.fileSystem.listStatus(new Path(baseDest, "tmp")).exists(_.getPath.getName == "items") should be (true)
        flow.flowContext.fileSystem.listFiles(new Path(baseDest, "items"), true).count(_.getPath.getName.startsWith("part")) should be (3)
        flow.flowContext.fileSystem.listFiles(new Path(new Path(baseDest, "tmp"), "items"), true).count(_.getPath.getName.startsWith("part")) should be (3)

      }

      it("commit a repartitioned parquet and make sure one label is not cached if it is not used as input elsewhere") {
        val spark = sparkSession
        import spark.implicits._
        spark.conf.set(SparkDataFlow.REMOVE_TEMP_AFTER_EXECUTION, false)

        val baseDest = testingBaseDir + "/dest"

        val flow = Waimak.sparkFlow(spark, s"$baseDest/tmp")
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "items")
          .commit("commit", 3)("items")
          .push("commit")(ParquetDataCommitter(baseDest))

        val (ex, _) = Waimak.sparkExecutor().execute(flow)

        spark.read.parquet(s"$baseDest/items").as[TPurchase].collect() should contain theSameElementsAs purchases

        // Check labels written out and count
        flow.flowContext.fileSystem.listStatus(new Path(baseDest, "tmp")).exists(_.getPath.getName == "items") should be (false)
        flow.flowContext.fileSystem.listFiles(new Path(baseDest, "items"), true).count(_.getPath.getName.startsWith("part")) should be (3)

      }

      it("commit a parquet and make sure one label is cached if it is used in multiple commit groups") {
        val spark = sparkSession
        import spark.implicits._

        val baseDest1 = testingBaseDir + "/dest1"
        val baseDest2 = testingBaseDir + "/dest2"
        val baseDest3 = testingBaseDir + "/dest3"

        val flow = Waimak.sparkFlow(spark, s"$baseDest1/tmp")
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "items")
          .commit("commit1")("items")
          .commit("commit2")("items")
          .commit("commit3")("items")
          .push("commit1")(ParquetDataCommitter(baseDest1).withSnapshotFolder("generated_timestamp=20180509094500"))
          .push("commit2")(ParquetDataCommitter(baseDest2).withSnapshotFolder("generated_timestamp=20180509094500"))
          .push("commit3")(ParquetDataCommitter(baseDest3).withSnapshotFolder("generated_timestamp=20180509094500"))

        val (ex, _) = Waimak.sparkExecutor().execute(flow)

        // Check to see if the alias action has been intercepted
        val interceptorAction = ex.filter(_.outputLabels.contains("items")).head
        interceptorAction shouldBe a[PostActionInterceptor[_]]

        // Check the post action is a cache
        val typedInterceptorAction = interceptorAction.asInstanceOf[PostActionInterceptor[Dataset[_]]]
        typedInterceptorAction.postActions.length should be(1)
        typedInterceptorAction.postActions.head shouldBe a[CachePostAction[_]]

        spark.read.parquet(s"$baseDest1/items").as[TPurchase].collect() should be(purchases)
        spark.read.parquet(s"$baseDest2/items").as[TPurchase].collect() should be(purchases)
        spark.read.parquet(s"$baseDest3/items").as[TPurchase].collect() should be(purchases)
      }

      it("commit a parquet and make sure one label is not cached even though the hint was given as it is not used as input in another action") {
        val spark = sparkSession
        import spark.implicits._

        val baseDest = testingBaseDir + "/dest"

        val flow = Waimak.sparkFlow(spark, s"$baseDest/tmp")
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "items")
          .commit("commit")("items")
          .push("commit")(ParquetDataCommitter(baseDest).withSnapshotFolder("generated_timestamp=20180509094500"))

        val (ex, _) = Waimak.sparkExecutor().execute(flow)

        // Check alias action has not been intercepted
        val notInterceptorAction = ex.filter(_.outputLabels.contains("items")).head
        notInterceptorAction should not be an[PostActionInterceptor[_]]

        spark.read.parquet(s"$baseDest/items").as[TPurchase].collect() should be(purchases)
      }

      it("commit a parquet and make sure one label is not cached as the hint was false") {
        val spark = sparkSession
        spark.conf.set(CACHE_REUSED_COMMITTED_LABELS, false)
        import spark.implicits._

        val baseDest = testingBaseDir + "/dest"

        val flow = Waimak.sparkFlow(spark, s"$baseDest/tmp")
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "items")
          .commit("commit")("items")
          .push("commit")(ParquetDataCommitter(baseDest).withSnapshotFolder("generated_timestamp=20180509094500"))

        val (ex, _) = Waimak.sparkExecutor().execute(flow)

        // Check alias action has not been intercepted
        val notInterceptorAction = ex.filter(_.outputLabels.contains("items")).head
        notInterceptorAction should not be an[PostActionInterceptor[_]]

        spark.read.parquet(s"$baseDest/items").as[TPurchase].collect() should be(purchases)
      }

      it("commit a parquet and make sure one label is not cached as the hint was false but the label is used as input to another commit group") {
        val spark = sparkSession
        spark.conf.set(CACHE_REUSED_COMMITTED_LABELS, false)
        import spark.implicits._

        val baseDest = testingBaseDir + "/dest"

        val flow = Waimak.sparkFlow(spark, s"$baseDest/tmp")
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "items")
          .commit("commit")("items")
          .push("commit")(ParquetDataCommitter(baseDest).withSnapshotFolder("generated_timestamp=20180509094500"))
          .alias("items", "items_aliased")

        val (ex, _) = Waimak.sparkExecutor().execute(flow)

        // Check alias action has not been intercepted
        val notInterceptorAction = ex.filter(_.outputLabels.contains("items")).head
        notInterceptorAction should not be an[PostActionInterceptor[_]]

        spark.read.parquet(s"$baseDest/items").as[TPurchase].collect() should be(purchases)
      }

      it("commit a parquet and make sure one label is not cached as the hint was false but the label is used in multiple commit groups") {
        val spark = sparkSession
        spark.conf.set(CACHE_REUSED_COMMITTED_LABELS, false)
        import spark.implicits._

        val baseDest1 = testingBaseDir + "/dest1"
        val baseDest2 = testingBaseDir + "/dest2"

        val flow = Waimak.sparkFlow(spark, s"$baseDest1/tmp")
          .openCSV(basePath)("csv_1")
          .alias("csv_1", "items")
          .commit("commit1")("items")
          .commit("commit2")("items")
          .push("commit1")(ParquetDataCommitter(baseDest1).withSnapshotFolder("generated_timestamp=20180509094500"))
          .push("commit2")(ParquetDataCommitter(baseDest2).withSnapshotFolder("generated_timestamp=20180509094500"))

        val (ex, _) = Waimak.sparkExecutor().execute(flow)

        // Check alias action has not been intercepted
        val notInterceptorAction = ex.filter(_.outputLabels.contains("items")).head
        notInterceptorAction should not be an[PostActionInterceptor[_]]

        spark.read.parquet(s"$baseDest1/items").as[TPurchase].collect() should be(purchases)
        spark.read.parquet(s"$baseDest2/items").as[TPurchase].collect() should be(purchases)
      }

    }
  }

  describe("FSCleanUp") {

    describe("one table") {

      it("empty, table folder does not exist") {
        val baseDest = testingBaseDir + "/dest"
        val committer = dateBasedSnapshotCleanupStrategy("snapshotFolder", dateFormat, 3)(fileStatusToName)
        val fsCleanupAction = new FSCleanUp(baseDest
          , committer
          , List("table"))

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)

        fsCleanupAction.performAction(DataFlowEntities.empty, flow.flowContext) should be(Success(List.empty))
      }

      it("empty, table folder exists") {
        val baseDest = testingBaseDir + "/dest"
        val committer = dateBasedSnapshotCleanupStrategy("snapshotFolder", dateFormat, 3)(fileStatusToName)
        val fsCleanupAction = new FSCleanUp(baseDest
          , committer
          , List("table"))

        val tableFolder = new File(baseDest, "table")
        tableFolder.mkdirs()

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)

        fsCleanupAction.performAction(DataFlowEntities.empty, flow.flowContext) should be(Success(List.empty))

        tableFolder.exists() should be(true)
      }

      it("one snapshot folder") {
        val baseDest = testingBaseDir + "/dest"
        val committer = dateBasedSnapshotCleanupStrategy("snapshotFolder", dateFormat, 3)(fileStatusToName)
        val fsCleanupAction = new FSCleanUp(baseDest
          , committer
          , List("table"))

        val tableFolder = new File(baseDest, "table")
        tableFolder.mkdirs()
        initSnapshotFolders(tableFolder, snapshotFoldersSameDay.take(1))

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)

        fsCleanupAction.performAction(DataFlowEntities.empty, flow.flowContext) should be(Success(List.empty))

        new File(tableFolder, "snapshotFolder=20181101_123001_567").exists() should be(true)
      }

      it("all snapshot folders") {
        val baseDest = testingBaseDir + "/dest"
        val committer = dateBasedSnapshotCleanupStrategy("snapshotFolder", dateFormat, 3)(fileStatusToName)
        val fsCleanupAction = new FSCleanUp(baseDest
          , committer
          , List("table"))

        val tableFolder = new File(baseDest, "table")
        tableFolder.mkdirs()
        initSnapshotFolders(tableFolder, snapshotFoldersSameDay)

        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)

        tableFolder.list().sorted should be(snapshotFoldersSameDay)

        fsCleanupAction.performAction(DataFlowEntities.empty, flow.flowContext) should be(Success(List.empty))

        tableFolder.list().sorted should be(snapshotFoldersSameDay.takeRight(3).toArray)
      }
    }

    describe("multiple tables") {

      it("all empty, no folders exist") {
        val baseDest = testingBaseDir + "/dest"
        val committer = dateBasedSnapshotCleanupStrategy("snapshotFolder", dateFormat, 3)(fileStatusToName)
        val fsCleanupAction = new FSCleanUp(baseDest
          , committer
          , List("table_1", "table_2", "table_3"))
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)

        fsCleanupAction.performAction(DataFlowEntities.empty, flow.flowContext) should be(Success(List.empty))
      }

      it("1 empty, 1 has 2 snapshots, 1 has 5 snapshots") {
        val baseDest = testingBaseDir + "/dest"
        val committer = dateBasedSnapshotCleanupStrategy("snapshotFolder", dateFormat, 3)(fileStatusToName)
        val fsCleanupAction = new FSCleanUp(baseDest
          , committer
          , List("table_empty", "table_with_one", "table_with_five"))
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)

        val table_empty = new File(baseDest, "table_empty")
        table_empty.mkdirs()

        initSnapshotFolders(new File(baseDest, "table_with_one"), snapshotFoldersSameDay.take(1))
        initSnapshotFolders(new File(baseDest, "table_with_five"), snapshotFoldersSameDay.take(5))


        fsCleanupAction.performAction(DataFlowEntities.empty, flow.flowContext) should be(Success(List.empty))

        table_empty.exists() should be(true)
        flow.flowContext.fileSystem.listStatus(new Path(baseDest + "/table_with_one")).map(_.getPath.getName) should be(snapshotFoldersSameDay.take(1))
        flow.flowContext.fileSystem.listStatus(new Path(baseDest + "/table_with_five")).map(_.getPath.getName).sorted should be(snapshotFoldersSameDay.slice(2, 5))
      }
    }
  }
}
