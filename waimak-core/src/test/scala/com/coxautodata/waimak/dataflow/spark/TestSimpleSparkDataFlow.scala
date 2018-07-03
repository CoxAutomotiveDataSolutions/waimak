package com.coxautodata.waimak.dataflow.spark

import java.io.File

import com.coxautodata.waimak.dataflow.{ActionResult, _}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.spark.sql.{AnalysisException, Dataset}
import org.apache.spark.sql.functions._

import scala.util.Try

/**
  * Created by Alexei Perelighin on 22/12/17.
  */
class TestSimpleSparkDataFlow extends SparkAndTmpDirSpec {

  override val appName: String = "Simple Spark Data Flow"

  val executor = Waimak.sparkExecutor()

  // Need to explicitly use sequential executor
  val sequentialExecutor = new SequentialDataFlowExecutor[SparkFlowContext](SparkFlowReporter)

  import SparkActions._
  import TestSparkData._

  describe("csv") {

    it("read one") {
      val spark = sparkSession
      import spark.implicits._

      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1")
        .show("csv_1")

      val (executedActions, finalState) = executor.execute(flow)

      executedActions.foreach(d => println(d.logLabel))

      //validate executed actions
      executedActions.size should be(2)
      executedActions.map(a => a.description) should be(Seq("Action: read Inputs: [] Outputs: [csv_1]", "Action: show Inputs: [csv_1] Outputs: []"))

      finalState.actions.size should be(0) // no actions to execute
      finalState.inputs.size should be(1)
      finalState.inputs.getOption[Dataset[_]]("csv_1").map(_.as[TPurchase].collect()).get should be(purchases)
    }

    it("read two, without prefix") {
      val spark = sparkSession
      import spark.implicits._

      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1", "csv_2")
        .show("csv_1")
        .show("csv_2")

      val (executedActions, finalState) = executor.execute(flow)

      //validate executed actions
      executedActions.size should be(4)
      executedActions.map(a => a.description) should be(Seq("Action: read Inputs: [] Outputs: [csv_1]", "Action: read Inputs: [] Outputs: [csv_2]", "Action: show Inputs: [csv_1] Outputs: []", "Action: show Inputs: [csv_2] Outputs: []"))

      finalState.actions.size should be(0) // no actions to execute
      finalState.inputs.size should be(2)
      finalState.inputs.getOption[Dataset[_]]("csv_1").map(_.as[TPurchase].collect()).get should be(purchases)
      finalState.inputs.getOption[Dataset[_]]("csv_2").map(_.as[TPerson].collect()).get should be(persons)
    }

    it("read two, with prefix") {
      val spark = sparkSession
      import spark.implicits._
      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath, None, Some("pr"))("csv_1", "csv_2")
        .show("pr_csv_1")
        .show("pr_csv_2")

      val (executedActions, finalState) = executor.execute(flow)

      //validate executed actions
      executedActions.size should be(4)
      executedActions.map(a => a.description) should be(Seq("Action: read Inputs: [] Outputs: [pr_csv_1]", "Action: read Inputs: [] Outputs: [pr_csv_2]", "Action: show Inputs: [pr_csv_1] Outputs: []", "Action: show Inputs: [pr_csv_2] Outputs: []"))

      finalState.actions.size should be(0) // no actions to execute
      finalState.inputs.size should be(2)
      finalState.inputs.getOption[Dataset[_]]("pr_csv_1").map(_.as[TPurchase].collect()).get should be(purchases)
      finalState.inputs.getOption[Dataset[_]]("pr_csv_2").map(_.as[TPerson].collect()).get should be(persons)
    }

    it("read path") {
      val spark = sparkSession
      import spark.implicits._
      val flow = Waimak.sparkFlow(spark)
        .openFileCSV(s"$basePath/csv_1", "csv_1")
        .show("csv_1")

      val (executedActions, finalState) = executor.execute(flow)

      //validate executed actions
      executedActions.size should be(2)
      executedActions.map(a => a.description) should be(Seq("Action: read Inputs: [] Outputs: [csv_1]", "Action: show Inputs: [csv_1] Outputs: []"))

      finalState.actions.size should be(0) // no actions to execute
      finalState.inputs.size should be(1)
      finalState.inputs.getOption[Dataset[_]]("csv_1").map(_.as[TPurchase].collect()).get should be(purchases)
    }
  }

  describe("readParquet") {

    it("read two parquet folders") {
      val spark = sparkSession
      import spark.implicits._
      val baseDest = testingBaseDir + "/dest"
      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "parquet_1")
        .alias("csv_2", "parquet_2")
        .writeParquet(baseDest)("parquet_1", "parquet_2")

      executor.execute(flow)

      val flow2 = Waimak.sparkFlow(spark)
        .openParquet(baseDest)("parquet_1", "parquet_2")

      val (executedActions, finalState) = executor.execute(flow2)
      finalState.inputs.getOption[Dataset[_]]("parquet_1").map(_.as[TPurchase].collect()).get should be(purchases)
      finalState.inputs.getOption[Dataset[_]]("parquet_2").map(_.as[TPerson].collect()).get should be(persons)

    }

    it("read two parquet folders with snapshot directory") {
      val spark = sparkSession
      import spark.implicits._
      val baseDest = testingBaseDir + "/dest"
      val flow = Waimak.sparkFlow(spark, s"$baseDest/tmp")
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "parquet_1")
        .alias("csv_2", "parquet_2")
        .stageAndCommitParquet(baseDest, snapshotFolder = Some("generated_timestamp=20180509094500"))("parquet_1", "parquet_2")

      executor.execute(flow)

      val flow2 = Waimak.sparkFlow(spark)
        .openParquet(baseDest, snapshotFolder = Some("generated_timestamp=20180509094500"))("parquet_1", "parquet_2")

      val (executedActions, finalState) = executor.execute(flow2)
      finalState.inputs.getOption[Dataset[_]]("parquet_1").map(_.as[TPurchase].collect()).get should be(purchases)
      finalState.inputs.getOption[Dataset[_]]("parquet_2").map(_.as[TPerson].collect()).get should be(persons)

    }

    it("stage and commit parquet, and force a cache as parquet") {
      val spark = sparkSession
      import spark.implicits._
      val baseDest = testingBaseDir + "/dest"
      val flow = Waimak.sparkFlow(spark, s"$baseDest/tmp")
        .openCSV(basePath)("csv_1")
        .alias("csv_1", "parquet_1")
        .cacheAsParquet("parquet_1")
        .inPlaceTransform("parquet_1")(df => df)
        .inPlaceTransform("parquet_1")(df => df)
        .stageAndCommitParquet(baseDest, snapshotFolder = Some("generated_timestamp=20180509094500"))("parquet_1")


      // Check all post actions made it through
      val interceptorAction = flow.actions.filter(_.outputLabels.contains("parquet_1")).head.asInstanceOf[PostActionInterceptor[Dataset[_], SparkFlowContext]]
      interceptorAction.postActions.length should be(3)

      // Check they are in the right order
      interceptorAction.postActions.head.isInstanceOf[TransformPostAction[Dataset[_], SparkFlowContext]] should be(true)
      interceptorAction.postActions.tail.head.isInstanceOf[TransformPostAction[Dataset[_], SparkFlowContext]] should be(true)
      interceptorAction.postActions.tail.tail.head.isInstanceOf[CachePostAction[Dataset[_], SparkFlowContext]] should be(true)

      executor.execute(flow)

      val flow2 = Waimak.sparkFlow(spark)
        .openParquet(baseDest, snapshotFolder = Some("generated_timestamp=20180509094500"))("parquet_1")

      val (_, finalState) = executor.execute(flow2)
      finalState.inputs.getOption[Dataset[_]]("parquet_1").map(_.as[TPurchase].collect()).get should be(purchases)

    }

  }

  describe("sql") {

    it("group by single") {
      val spark = sparkSession
      import spark.implicits._
      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1")
        .sql("csv_1")("person_summary", "select id, count(item) as item_cnt, sum(amount) as total from csv_1 group by id")
        .show("person_summary")

      val (executedActions, finalState) = executor.execute(flow)

      //validate executed actions
      executedActions.size should be(3)
      executedActions.map(a => a.description) should be(Seq("Action: read Inputs: [] Outputs: [csv_1]", "Action: sql Inputs: [csv_1] Outputs: [person_summary]", "Action: show Inputs: [person_summary] Outputs: []"))

      finalState.actions.size should be(0) // no actions to execute
      finalState.inputs.size should be(2)
      finalState.inputs.getOption[Dataset[_]]("person_summary").map(_.as[TSummary].collect()).get should be(Seq(
        TSummary(Some(1), Some(3), Some(7))
        , TSummary(Some(3), Some(1), Some(2))
        , TSummary(Some(5), Some(3), Some(3))
        , TSummary(Some(2), Some(1), Some(2))
      ))
    }

    it("group by and join with drop of columns") {
      val spark = sparkSession
      import spark.implicits._
      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1", "csv_2")
        .sql("csv_1")("person_summary", "select id as id_summary, count(item) as item_cnt, sum(amount) as total from csv_1 group by id") // give unique names so that after joins it is easy to drop
        .sql("person_summary", "csv_2")("report_tmp",
        """
          |select * from csv_2 person
          |left join person_summary on person.id = id_summary""".stripMargin
        , "id_summary")
        .transform("report_tmp")("report") { report_tmp => report_tmp.withColumn("calc_1", lit(2)) }
        .printSchema("report")
        .show("report")

      val (executedActions, finalState) = executor.execute(flow)
      //      executedActions.foreach(a => println(a.logLabel))
      //validate executed actions
      executedActions.size should be(7)
      executedActions.map(a => a.description) should be(Seq(
        "Action: read Inputs: [] Outputs: [csv_1]"
        , "Action: read Inputs: [] Outputs: [csv_2]"
        , "Action: sql Inputs: [csv_1] Outputs: [person_summary]"
        , "Action: sql Inputs: [person_summary,csv_2] Outputs: [report_tmp]"
        , "Action: transform 1 -> 1 Inputs: [report_tmp] Outputs: [report]"
        , "Action: printSchema Inputs: [report] Outputs: []"
        , "Action: show Inputs: [report] Outputs: []"
      ))

      finalState.actions.size should be(0) // no actions to execute
      finalState.inputs.size should be(5)
      finalState.inputs.getOption[Dataset[_]]("report").map(_.as[TReport].collect()).get should be(report)
    }
  }

  describe("joins") {

    it("2 drop") {
      val spark = sparkSession
      import spark.implicits._
      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1", "csv_2")
        .sql("csv_1")("person_summary", "select id, count(item) as item_cnt, sum(amount) as total from csv_1 group by id")
        .transform("csv_2", "person_summary")("report") { (l, r) => l.join(r, l("id") === r("id"), "left").drop(r("id")) }
        .printSchema("report")
        .show("report")

      val (executedActions, finalState) = executor.execute(flow)
      //            executedActions.foreach(a => println(a.logLabel))
      //validate executed actions
      executedActions.size should be(6)
      executedActions.map(a => a.description) should be(Seq(
        "Action: read Inputs: [] Outputs: [csv_1]"
        , "Action: read Inputs: [] Outputs: [csv_2]"
        , "Action: sql Inputs: [csv_1] Outputs: [person_summary]"
        , "Action: transform 2 -> 1 Inputs: [csv_2,person_summary] Outputs: [report]"
        , "Action: printSchema Inputs: [report] Outputs: []"
        , "Action: show Inputs: [report] Outputs: []"
      ))

      finalState.actions.size should be(0) // no actions to execute
      finalState.inputs.size should be(4)
      finalState.inputs.getOption[Dataset[_]]("report").map(_.withColumn("calc_1", lit(2)).as[TReport].collect()).get should be(report)
    }

  }

  describe("transform") {

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
      //            executedActions.foreach(a => println(a.logLabel))
      //validate executed actions
      executedActions.size should be(6)
      executedActions.map(a => a.description) should be(Seq(
        "Action: read Inputs: [] Outputs: [csv_1]"
        , "Action: read Inputs: [] Outputs: [csv_2]"
        , "Action: transform 1 -> 1 Inputs: [csv_1] Outputs: [person_summary]"
        , "Action: transform 2 -> 1 Inputs: [person_summary,csv_2] Outputs: [report]"
        , "Action: printSchema Inputs: [report] Outputs: []"
        , "Action: show Inputs: [report] Outputs: []"
      ))

      finalState.actions.size should be(0) // no actions to execute
      finalState.inputs.size should be(4)
      finalState.inputs.getOption[Dataset[_]]("report").map(_.as[TReport].collect()).get should be(report)
    }
  }

  describe("stageAndCommitParquet") {

    it("stage csv to parquet and commit") {
      val spark = sparkSession
      import spark.implicits._
      val baseDest = testingBaseDir + "/dest"

      val flow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "items")
        .alias("csv_2", "person")
        .stageAndCommitParquet(baseDest, partitions = Seq("amount"))("items")
        .stageAndCommitParquet(baseDest, snapshotFolder = Some("generatedTimestamp=2018-03-13-16-19-00"))("person")

      val (executedActions, finalState) = executor.execute(flow)
      finalState.inputs.size should be(4)
      new File(s"$baseDest/items").list().toSeq.filter(_.startsWith("amount=")).sorted should be(Seq("amount=1", "amount=2", "amount=4"))
      spark.read.parquet(s"$baseDest/items").as[TPurchase].sort("id", "item", "amount").collect() should be(purchases)
      spark.read.parquet(s"$baseDest/person/generatedTimestamp=2018-03-13-16-19-00").as[TPerson].collect() should be(persons)
    }

    it("stage csv to parquet on an action that returns two labels and commit") {
      val spark = sparkSession
      import spark.implicits._
      val baseDest = testingBaseDir + "/dest"

      val flow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1", "csv_2")
        .addAction(new TestTwoInputsAndOutputsAction(List("csv_1", "csv_2"), List("items", "person"), (_, _)))
        .stageAndCommitParquet(baseDest, partitions = Seq("amount"))("items")
        .stageAndCommitParquet(baseDest, snapshotFolder = Some("generatedTimestamp=2018-03-13-16-19-00"))("person")

      val (executedActions, finalState) = executor.execute(flow)
      finalState.inputs.size should be(4)
      new File(s"$baseDest/items").list().toSeq.filter(_.startsWith("amount=")).sorted should be(Seq("amount=1", "amount=2", "amount=4"))
      spark.read.parquet(s"$baseDest/items").as[TPurchase].sort("id", "item", "amount").collect() should be(purchases)
      spark.read.parquet(s"$baseDest/person/generatedTimestamp=2018-03-13-16-19-00").as[TPerson].collect() should be(persons)
    }

    it("stage csv to parquet and commit should throw exception when dest exists") {
      val spark = sparkSession
      val baseDest = testingBaseDir + "/dest"
      val resultDest = testingBaseDir + "/dest/person/generatedTimestamp=2018-03-13-16-19-00"
      FileUtils.forceMkdir(new File(resultDest))

      val flow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_2")
        .alias("csv_2", "person")
        .stageAndCommitParquet(baseDest, snapshotFolder = Some("generatedTimestamp=2018-03-13-16-19-00"))("person")

      val res = intercept[DataFlowException] {
        executor.execute(flow)
      }
      res.text.split(": ", 3).zipWithIndex.collect { case (s, i) if i != 1 => s }.mkString(" ") should be("Exception performing action Action: CommitAction Inputs: [csv_2,person] Outputs: []")
      res.cause shouldBe a[FileAlreadyExistsException]
    }

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
      executor.execute(flow)

    }

  }

  describe("write") {

    it("writeCSV") {
      val spark = sparkSession
      import spark.implicits._
      val baseDest = testingBaseDir + "/dest"

      val flow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "items")
        .alias("csv_2", "person")
        .writeCSV(baseDest, Map("header" -> "true"))("person")
        .writePartitionedCSV(baseDest, options = Map("header" -> "true"))("items", "amount")

      val (executedActions, finalState) = executor.execute(flow)
      finalState.inputs.size should be(4)
      new File(s"$baseDest/items").list().toSeq.filter(_.startsWith("amount=")).sorted should be(Seq("amount=1", "amount=2", "amount=4"))
      spark.read.option("inferSchema", "true").option("header", "true").csv(s"$baseDest/items").as[TPurchase].sort("id", "item", "amount").collect() should be(purchases)
      spark.read.option("inferSchema", "true").option("header", "true").csv(s"$baseDest/person").as[TPerson].collect() should be(persons)
    }

    it("writeCSV multiple with overwrite") {
      val spark = sparkSession
      import spark.implicits._
      val baseDest = testingBaseDir + "/dest"
      val dummyPath = new File(s"$baseDest/dummy")
      val itemsPath = new File(s"$baseDest/items")

      FileUtils.forceMkdir(dummyPath)
      dummyPath.exists() should be(true)
      itemsPath.exists() should be(false)

      val flow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "items")
        .alias("csv_2", "person")
        .writeCSV(baseDest, Map("header" -> "true"), overwrite = true)("person", "items")

      val (executedActions, finalState) = executor.execute(flow)
      finalState.inputs.size should be(4)
      spark.read.option("inferSchema", "true").option("header", "true").csv(s"$baseDest/items").as[TPurchase].collect() should be(purchases)
      spark.read.option("inferSchema", "true").option("header", "true").csv(s"$baseDest/person").as[TPerson].collect() should be(persons)

      dummyPath.exists() should be(true)
      itemsPath.exists() should be(true)
    }

    it("writeParquet") {
      val spark = sparkSession
      import spark.implicits._
      val baseDest = testingBaseDir + "/dest"

      val flow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "items")
        .alias("csv_2", "person")
        .writeParquet(baseDest)("person")
        .writePartitionedParquet(baseDest)("items", "amount")

      val (executedActions, finalState) = executor.execute(flow)
      finalState.inputs.size should be(4)
      new File(s"$baseDest/items").list().toSeq.filter(_.startsWith("amount=")).sorted should be(Seq("amount=1", "amount=2", "amount=4"))
      spark.read.parquet(s"$baseDest/items").as[TPurchase].sort("id", "item", "amount").collect() should be(purchases)
      spark.read.parquet(s"$baseDest/person").as[TPerson].collect() should be(persons)
    }

    it("writeParquet with multiple labels") {
      val spark = sparkSession
      import spark.implicits._
      val baseDest = testingBaseDir + "/dest"

      val flow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "items")
        .alias("csv_2", "person")
        .writeParquet(baseDest)("person", "items")

      val (executedActions, finalState) = executor.execute(flow)
      finalState.inputs.size should be(4)
      spark.read.parquet(s"$baseDest/items").as[TPurchase].collect() should be(purchases)
      spark.read.parquet(s"$baseDest/person").as[TPerson].collect() should be(persons)
    }

    describe("tag and tagDependency") {

      it("missing tag") {
        val spark = sparkSession
        val baseDest = testingBaseDir + "/dest"

        val flow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .tagDependency("write_person", "write_items") {
            _.openParquet(baseDest)("person_written", "items_written")
          }
          .openCSV(basePath)("csv_1", "csv_2")
          .alias("csv_1", "items")
          .alias("csv_2", "person")
          .writeParquet(baseDest)("person", "items")

        val res = intercept[DataFlowException] {
          sequentialExecutor.execute(flow)
        }
        res.text should be(s"Could not find any actions tagged with label [write_person] when resolving dependent actions for action [${flow.actions.head.guid}]")

      }

      it("no tag dependencies, should be missing file") {
        // This will produce a file missing exception if no tag dependencies introduced
        val spark = sparkSession
        val baseDest = testingBaseDir + "/dest"

        val flow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .openParquet(baseDest)("person_written", "items_written")
          .openCSV(basePath)("csv_1", "csv_2")
          .alias("csv_1", "items")
          .alias("csv_2", "person")
          .writeParquet(baseDest)("person", "items")

        val res = intercept[DataFlowException] {
          sequentialExecutor.execute(flow)
        }
        res.cause shouldBe a[AnalysisException]
        res.cause.asInstanceOf[AnalysisException].message should be(s"Path does not exist: file:$baseDest/person_written")
      }

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

        val (_, finalState) = sequentialExecutor.execute(flow)
        finalState.inputs.size should be(6)

        finalState.inputs.get[Dataset[_]]("items_written").as[TPurchase].collect() should be(purchases)
        finalState.inputs.get[Dataset[_]]("person_written").as[TPerson].collect() should be(persons)

      }

      it("cyclic dependency tags") {

        val spark = sparkSession
        val baseDest = testingBaseDir + "/dest"

        val flow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .tagDependency("written") {
            _.tag("read") {
              _.openParquet(baseDest)("person_written")
            }
          }
          .openCSV(basePath)("csv_2")
          .alias("csv_2", "person")
          .tagDependency("read") {
            _.tag("written") {
              _.writeParquet(baseDest)("person")
            }
          }

        val res = intercept[DataFlowException] {
          sequentialExecutor.execute(flow)
        }
        res.text should be(s"Circular reference for action [${flow.actions.find(_.inputLabels.contains("person")).get.guid}] as a result of cyclic tag dependency. " +
          "Action has the following tag dependencies [read] and depends on the following input labels [person]")
      }

      it("tag dependency conflicting with input dependency") {
        val spark = sparkSession
        import spark.implicits._
        val baseDest = testingBaseDir + "/dest"

        val flow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .tagDependency("written") {
            _.openFileParquet(s"$baseDest/person", "person_written")
              .openFileParquet(s"$baseDest/items", "items_written")
          }
          .openCSV(basePath)("csv_1", "csv_2")
          .tagDependency("written") {
            _.alias("csv_1", "items")
          }
          .alias("csv_2", "person")
          .tag("written") {
            _.writeParquet(baseDest)("person", "items")
          }

        val res = intercept[DataFlowException] {
          sequentialExecutor.execute(flow)
        }
        res.text should be(s"Circular reference for action [${flow.actions.find(_.inputLabels.contains("csv_1")).get.guid}] as a result of cyclic tag dependency. " +
          "Action has the following tag dependencies [written] and depends on the following input labels [csv_1]")

      }

      it("tag dependent action depends on an action that does not run and therefore does not run") {
        val spark = sparkSession
        import spark.implicits._
        val baseDest = testingBaseDir + "/dest"

        val flow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
          .addInput("test_1", None)
          .tag("tag1") {
            _.transform("test_1")("test_2")(df => df)
          }
          .tagDependency("tag1") {
            _.openCSV(basePath)("csv_1")
          }

        val (executed, _) = sequentialExecutor.execute(flow)
        executed.size should be(0)

      }

    }

  }


  describe("debugAsTable") {

    it("sql") {
      val spark = sparkSession
      import spark.implicits._
      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1", "csv_2")
        .debugAsTable("csv_1", "csv_2")

      executor.execute(flow)

      val csv_1_sql = spark.sql("select * from csv_1")
      csv_1_sql.as[TPurchase].collect() should be(purchases)

      val csv_2_sql = spark.sql("select * from csv_2")
      csv_2_sql.as[TPerson].collect() should be(persons)
    }

  }

  describe("map/mapOption") {
    it("map should transform a sparkdataflow when using implicit classes") {

      val emptyFlow: SimpleSparkDataFlow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
      implicit class TestSparkImplicit1(dataFlow: SparkDataFlow) {
        def runTest1: SparkDataFlow = dataFlow.addAction(new TestEmptySparkAction(List.empty, List.empty) {
          override val guid: String = "abd22c36-4dd0-4fa5-9298-c494ede7f363"
        })
      }

      implicit class TestSparkImplicit2(dataFlow: SparkDataFlow) {
        def runTest2: SparkDataFlow = dataFlow.addAction(new TestEmptySparkAction(List.empty, List.empty) {
          override val guid: String = "f40ee6fa-157b-4d65-ad7a-17639da403bf"
        })
      }

      emptyFlow.map(f => if (true) f.runTest1 else f).runTest2.actions.map(_.guid) should be(Seq("abd22c36-4dd0-4fa5-9298-c494ede7f363", "f40ee6fa-157b-4d65-ad7a-17639da403bf"))

    }
  }

  describe("SequentialDataFlowExecutor") {
    it("any files in staging dir should be cleaned up before any actions are executed") {

      val testingDir = new File(tmpDir.toUri.getPath + "/test1")
      FileUtils.forceMkdir(testingDir)
      testingDir.getParentFile.list().toSeq should be(Seq("test1"))

      val emptyFlow: SimpleSparkDataFlow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
      executor.execute(emptyFlow)
      testingDir.getParentFile.list().toSeq should be(Seq())

    }

    it("any empty staging folder should be created when a flow is executed") {

      val tmpDirFile = new File(tmpDir.toUri.getPath)
      tmpDirFile.getParentFile.list().toSeq should be(Seq())

      val emptyFlow: SimpleSparkDataFlow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
      executor.execute(emptyFlow)
      tmpDirFile.list().toSeq should be(Seq())

    }
  }


  describe("mixing multiple types in flow") {

    it("should handle multiple types in the flow") {

      val emptyFlow = SimpleSparkDataFlow.empty(sparkSession, tmpDir)
      val flow = emptyFlow.addInput("integer_1", Some(1))
        .addInput("dataset_1", Some(sparkSession.emptyDataFrame))
        .addAction(new TestOutputMultipleTypesAction(List("integer_1", "dataset_1"), List("integer_2", "dataset_2"),
          (i, ds) => (i + 1, ds)))

      val res = executor.execute(flow)
      res._2.inputs.get[Int]("integer_2") should be(2)
      res._2.inputs.get[Dataset[_]]("dataset_2") should be(sparkSession.emptyDataFrame)
    }
  }
}

class TestEmptySparkAction(val inputLabels: List[String], val outputLabels: List[String]) extends SparkDataFlowAction {

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try(List.empty)

}

class TestTwoInputsAndOutputsAction(override val inputLabels: List[String], override val outputLabels: List[String], run: (Dataset[_], Dataset[_]) => (Dataset[_], Dataset[_])) extends SparkDataFlowAction {

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try {
    if (inputLabels.length != 2 && outputLabels.length != 2) throw new IllegalArgumentException("Number of input label and output labels must be 2")
    val res: (Dataset[_], Dataset[_]) = run(inputs.get[Dataset[_]](inputLabels(0)), inputs.get[Dataset[_]](inputLabels(1)))
    Seq(Some(res._1), Some(res._2))
  }
}


class TestOutputMultipleTypesAction(override val inputLabels: List[String]
                                    , override val outputLabels: List[String]
                                    , run: (Int, Dataset[_]) => (Int, Dataset[_])) extends SparkDataFlowAction {

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try {
    val res: (Int, Dataset[_]) = run(inputs.get[Int](inputLabels(0)), inputs.get[Dataset[_]](inputLabels(1)))
    Seq(Some(res._1), Some(res._2))
  }
}
