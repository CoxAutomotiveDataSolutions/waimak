package org.apache.spark

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.TestSparkData.basePath
import com.coxautodata.waimak.dataflow.spark.ui.{WaimakEdge, WaimakGraph, WaimakNode}
import com.coxautodata.waimak.dataflow.spark.{SimpleSparkDataFlow, SparkAndTmpDirSpec, TestTwoInputsAndOutputsAction}
import org.apache.spark.sql.functions.{count, lit, sum}
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
          a.count;
          a
        }, {
          b.count;
          b
        })))

      //val (executedActions, finalState) = executor.execute(flow)
      //Thread.sleep(1000000000L)
      //Thread.sleep(30000L) // 30secs
      val graph = WaimakGraph(flow)

      graph should be(WaimakGraph(List(
        WaimakNode(0, "read", List(), List("csv_1"), Set(), Set(), List()),
        WaimakNode(1, "read", List(), List("csv_2"), Set(), Set(), List()),
        WaimakNode(2, "show", List("csv_1"), List(), Set(), Set(), List()),
        WaimakNode(3,"TestTwoInputsAndOutputsAction",List("csv_1", "csv_2"),List("csv_1_a", "csv_2_a"),Set(),Set(),List())
      ), List(
        WaimakEdge(0,2),
        WaimakEdge(0,3),
        WaimakEdge(1,3))))
    }

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

      //val (executedActions1, finalState1) = executor.execute(flow1)
      //Thread.sleep(1000000000L)
      //Thread.sleep(300000L)// 5mins
      //Thread.sleep(120000L) // 2mins
      //Thread.sleep(60000L) // 1min
      //Thread.sleep(30000L) // 30secs

      val graph = WaimakGraph(flow1)

      graph should be(WaimakGraph(List(
          WaimakNode(0, "read", List(), List("csv_1"), Set(), Set(), List()),
          WaimakNode(1, "read", List(), List("csv_2"), Set(), Set(), List()),
          WaimakNode(2, "show", List("csv_1"), List(), Set(), Set(), List()),
          WaimakNode(3, "show", List("csv_2"), List(), Set(), Set(), List()),
          WaimakNode(4, "show", List("csv_1"), List(), Set(), Set(), List()),
          WaimakNode(5, "TestTwoInputsAndOutputsAction", List("csv_1",  "csv_2"), List("csv_1_a",  "csv_2_a"), Set(), Set(), List())
        ), List(
          WaimakEdge(0, 2),
          WaimakEdge(0, 4),
          WaimakEdge(0, 5),
          WaimakEdge(1, 3),
          WaimakEdge(1, 5))))
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

/*      val (executedActions, finalState) = executor.execute(flow)
      Thread.sleep(30000L) // 30secs*/

/*      val graph = WaimakGraph(flow)

      graph should be(WaimakGraph(List()))*/
      val graph = WaimakGraph(flow)

      graph should be(WaimakGraph(List(
        WaimakNode(2, "read", List(), List("csv_1"), Set(), Set(), List()),
        WaimakNode(3, "read", List(), List("csv_2"), Set(), Set(), List()),
        WaimakNode(4, "alias", List("csv_1"), List("items"), Set(), Set(), List()),
        WaimakNode(5, "alias", List("csv_2"), List("person"), Set(), Set(), List()),
        WaimakNode(8, "", List(), List(), Set(), Set("written"), List(
          WaimakNode(0, "read", List(), List("person_written"), Set(), Set(), List()),
          WaimakNode(1, "read", List(), List("items_written"), Set(), Set(), List()))),
        WaimakNode(9, "",  List(), List(), Set("written"), Set(), List(
          WaimakNode(6, "write", List("person"), List(), Set(), Set(), List()),
          WaimakNode(7, "write", List("items"), List(), Set(), Set(), List())))
      ), List(
        WaimakEdge(2, 4),
        WaimakEdge(3, 5),
        WaimakEdge(4, 7),
        WaimakEdge(5, 6),
        WaimakEdge(6, 0),
        WaimakEdge(6, 1),
        WaimakEdge(7, 0),
        WaimakEdge(7, 1))))
    }
/*

    it("stage and commit parquet, and force a cache as parquet") {
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
    }


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

    it(" First actual unit test"){
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

      val graph = WaimakGraph(flow)
/*
      graph should be(WaimakGraph(Seq(WaimakNode(0, "", Seq(""), Seq(""))), Seq(WaimakEdge(0,1))))*/
    }

*/
/*
    it("test on function makeDotNode"){

      val the = WaimakNode(0, "alias", List("csv_1"), List("csv_2")).makeDotNode
      //println("theNode=", theDotNode)
      theDotNode should be(
        s"""|  0 [label="ALIAS
            |Inputs:         csv_1
            |Outputs:      csv_2"];""".stripMargin)
    }
*/

    it("test on function makeDotNode"){

      val theDotNode = WaimakNode(0, "alias", List("csv_1"), List("csv_2")).makeDotNode
      //println("theNode=", theDotNode)
      theDotNode should be(
        s"""|  0 [label="ALIAS
           |Inputs:         csv_1
           |Outputs:      csv_2"];""".stripMargin)
    }

    it("test on function clusterBoxString"){

      val theOuterDotNode = WaimakNode(1, "", Nil, Nil, Set("written"), Set(), Seq(WaimakNode(0, "alias", List("csv_1"), List("csv_2")))).makeOuterDotNode
      //println("theNode=", theOuterDotNode)
      theOuterDotNode should be(
        s"""|
  subgraph cluster1 {
    label="Tags: written";
      0 [label="ALIAS
Inputs:         csv_1
Outputs:      csv_2"];
  }
     """.stripMargin)
    }

/*    it("test on function makeDotFile"){

      val theWaimakGraph = WaimakGraph(Seq(
        WaimakNode(2, "", Nil, Nil, Set("written"), Set(), Seq(
          WaimakNode(0, "read", List(), List("csv_2")),
          WaimakNode(1, "alias", List("csv_1"), List("csv_2"))))),
      Seq(WaimakEdge(0, 1)))
        .makeDotFile

      println("theNode=", theWaimakGraph)
      theWaimakGraph should be(
        s"""digraph G {

  subgraph cluster2 {
    label="Tags: written";
      0 [label="READ
Inputs:
Outputs:      csv_2"];
  1 [label="ALIAS
Inputs:         csv_1
Outputs:      csv_2"];
  }

  0->1;

}""") //.stripMargin)
    }*/

  }
}