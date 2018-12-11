package com.coxautodata.waimak.dataflow.spark

import java.io.File

import com.coxautodata.waimak.dataflow._
import org.apache.hadoop.fs.{FileStatus, Path}
import ParquetDataCommitter._

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
    ,"snapshotFolder=20181104_123001_567"
    ,"snapshotFolder=20181105_123001_567"
    ,"snapshotFolder=20181106_123001_567"
    ,"snapshotFolder=20181107_123001_567"
    ,"snapshotFolder=20181108_123001_567"
    ,"snapshotFolder=20181109_123001_567"
    ,"snapshotFolder=20181110_123001_567"
    ,"snapshotFolder=20181111_123001_567"
  )

  val snapshotFoldersSameDay = Seq(
    "snapshotFolder=20181101_123001_567"
    , "snapshotFolder=20181101_123001_568"
    , "snapshotFolder=20181101_123001_569"
    ,"snapshotFolder=20181101_123003_567"
    ,"snapshotFolder=20181101_123041_567"
    ,"snapshotFolder=20181101_123041_568"
    ,"snapshotFolder=20181101_123041_569"
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

      it("wrong type of the data flow") {
        val baseDest = testingBaseDir + "/dest"
        val emptyFlow = MockDataFlow.empty
        val committer = ParquetDataCommitter(baseDest)
        val res = committer.validate(emptyFlow, "f1", Seq.empty)
        res shouldBe a[Failure[_]]
        res.failed.get.getMessage should be(s"""ParquetDataCommitter [f1] can only work with data flows derived from ${classOf[SparkDataFlow].getName}""")
      }

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
        val committer = ParquetDataCommitter(baseDest).dateBasedSnapshotCleanup("snap", "YYYY", 1)
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        val res = committer.validate(flow, "f1", Seq.empty)
        res shouldBe a[Failure[_]]
        res.failed.get.getMessage should be("ParquetDataCommitter [f1], cleanup will only work when snapshot folder is defined")
      }

      it("snapshot folder already exists for 2 labels out of 3") {
        val baseDest = testingBaseDir + "/dest"
        val committer = ParquetDataCommitter(baseDest)
          .snapshotFolder("snap=2001")
          .dateBasedSnapshotCleanup("snap", "YYYY", 1)
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)

        initSnapshotFolders(new File(baseDest, "label_fail_1"), Seq("snap=2000", "snap=2001"))
        initSnapshotFolders(new File(baseDest, "label_fail_2"), Seq("snap=2000", "snap=2001"))
        initSnapshotFolders(new File(baseDest, "label_ok"), Seq("snap=2000"))

        val res = committer.validate(flow, "f1", Seq(
          CommitEntry("label_fail_1", "f1", Seq.empty, false)
          , CommitEntry("label_fail_2", "f1", Seq.empty, false)
          , CommitEntry("label_ok", "f1", Seq.empty, false))
        )
        res shouldBe a[Failure[_]]
        res.failed.get.getMessage should be("ParquetDataCommitter [f1], snapshot folder [snap=2001] is already present for labels: [label_fail_1, label_fail_2]")
      }
    }

    describe("success") {

      it("bare minimum") {
        val baseDest = testingBaseDir + "/dest"
        val committer = ParquetDataCommitter(baseDest)
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        committer.validate(flow, "fff", Seq(CommitEntry("label_1", "fff", Seq.empty, false))) should be(Success())
      }

      it("with snapshot and no cleanup") {
        val baseDest = testingBaseDir + "/dest"
        val committer = ParquetDataCommitter(baseDest).snapshotFolder("ts=777777")
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        committer.validate(flow, "fff", Seq(CommitEntry("label_1", "fff", Seq.empty, false))) should be(Success())
      }

      it("multiple labels, no clash of the snapshot folders of committed labels") {
        val baseDest = testingBaseDir + "/dest"
        val committer = ParquetDataCommitter(baseDest)
          .snapshotFolder("snap=2003")
          .dateBasedSnapshotCleanup("snap", "YYYY", 1)
        val flow: SparkDataFlow = Waimak.sparkFlow(sparkSession, tmpDir.toString)

        initSnapshotFolders(new File(baseDest, "label_not_committed"), Seq("snap=2000", "snap=2003"))
        initSnapshotFolders(new File(baseDest, "label_1"), Seq("snap=2000", "snap=2001"))
        initSnapshotFolders(new File(baseDest, "label_2"), Seq("snap=2000", "snap=2001"))
        initSnapshotFolders(new File(baseDest, "label_3"), Seq("snap=2000"))

        val res = committer.validate(flow, "f1", Seq(
          CommitEntry("label_1", "f1", Seq.empty, false)
          , CommitEntry("label_2", "f1", Seq.empty, false)
          , CommitEntry("label_3", "f1", Seq.empty, false)
          , CommitEntry("label_no_init", "f1", Seq.empty, false))
        )
        res should be(Success())
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
