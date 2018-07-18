package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.TestSparkData.basePath
import com.coxautodata.waimak.dataflow.spark.ui.{WaimakEdge, WaimakGraph, WaimakNode}

class TestWaimakGraph extends SparkAndTmpDirSpec {
  override val appName: String = "Spark UI"

  override val sparkOptions: Map[String, String] = Map("spark.executor.memory" -> "2g", "spark.ui.enabled" -> "true")

  val executor = Waimak.sparkExecutor()

  describe("Waimak tab graphs") {

    it("should be a graph of 4 WaimakNodes and 3 WaimakEdges") {
      val spark = sparkSession

      val flow = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1", "csv_2")
        .show("csv_1")
        .addAction(new TestTwoInputsAndOutputsAction(List("csv_1", "csv_2"), List("csv_1_a", "csv_2_a"), (a, b) => ( {
          a.count
          a
        }, {
          b.count
          b
        })))

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

    it("should be a graph of 6 WaimakNodes and 5 WaimakEdges") {
      val spark = sparkSession
      val flow1 = Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1", "csv_2")
        .show("csv_1")
        .show("csv_2")
        .show("csv_1")
        .addAction(new TestTwoInputsAndOutputsAction(List("csv_1", "csv_2"), List("csv_1_a", "csv_2_a"), (a, b) => ( {
          a.count; a
        }, {
          b.count; b
        })))

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

    it("should be a graph of ten WaimakNodes and 8 WaimakEdges") {

      val spark = sparkSession
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
  }

  describe("WaimakNode.makeDotNode") {

    it("should be a makeDotNode string"){

      val theDotNode = WaimakNode(0, "alias", List("csv_1"), List("csv_2")).makeDotNode

      theDotNode should be(
        s"""|  0 [label="ALIAS
            |Inputs:         csv_1
            |Outputs:      csv_2"];""".stripMargin)
    }
  }

  describe("WaimakNode.clusterBoxString") {

    it("should be a clusterBoxString string") {

      val theOuterDotNode = WaimakNode(1, "", Nil, Nil, Set("written"), Set(), Seq(WaimakNode(0, "alias", List("csv_1"), List("csv_2")))).makeOuterDotNode

      theOuterDotNode should be(
        s"""|
            |  subgraph cluster1 {
            |    label="Tags: written";
            |      0 [label="ALIAS
            |Inputs:         csv_1
            |Outputs:      csv_2"];
  }
     """.stripMargin)
    }
  }

  describe("WaimakGraph.makeDotFile") {

    it("should be a makeDotFile string"){

      val theWaimakGraph = WaimakGraph(Seq(
        WaimakNode(2, "", Nil, Nil, Set("written"), Set(), Seq(
          WaimakNode(0, "read", List(), List("csv_2")),//,
          WaimakNode(1, "alias", List("csv_1"), List("csv_2"))))),
      Seq(WaimakEdge(0, 1)))
        .makeDotFile

      theWaimakGraph // strip trailing whitespaces in each line before comparing
        .split("\n").map(_.reverse.dropWhile(_ == ' ').reverse).mkString("\n") should be
         s"""|digraph G {
             |
             |  subgraph cluster2 {
             |    label="Tags: written";
             |      0 [label="READ
             |Inputs:
             |Outputs:      csv_2"];
             |  1 [label="ALIAS
             |Inputs:         csv_1
             |Outputs:      csv_2"];
             |  }
             |
             |  0->1;
             |
             |}""".stripMargin
    }
  }
}