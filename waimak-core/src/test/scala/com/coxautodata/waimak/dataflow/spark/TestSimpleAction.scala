package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkActions._

/**
  * Created by Alexei Perelighin on 22/12/17.
  */
class TestSimpleAction extends SparkAndTmpDirSpec {

  override val appName: String = "Curry Action Tests"

  describe("initialise spark curry") {

    //No execution during initialisation.
    //Adding actions should not add inputs, as those are created during execution

    it("add one action, with one output") {
      val emptyFlow = Waimak.sparkFlow(sparkSession)
      val with2 = emptyFlow.openParquet("base/path")("table_1")
      with2.inputs.size should be(0)
      with2.actions(0).outputLabels should be(List("table_1"))
      with2.actions.size should be(1)
    }

    it("add one action, with 2 outputs") {
      val emptyFlow = Waimak.sparkFlow(sparkSession)
      val with2 = emptyFlow.openTable("test")("table_1", "table_2")
      with2.inputs.size should be(0)
      with2.actions(0).outputLabels should be(List("table_1", "table_2"))
      with2.actions.size should be(1)
    }

    it("add 2 actions, with 2 outputs") {
      val emptyFlow = Waimak.sparkFlow(sparkSession)
      val with2 = emptyFlow
        .openTable("test1")("table_1", "table_2")
        .openTable("test1")("table_3", "table_4")
      with2.inputs.size should be(0)
      with2.actions(0).outputLabels should be(List("table_1", "table_2"))
      with2.actions(1).outputLabels should be(List("table_3", "table_4"))
      with2.actions.size should be(2)
    }

    it("add one action, with prefix output") {
      val emptyFlow = Waimak.sparkFlow(sparkSession)
      val with2 = emptyFlow.openParquet("base/path", None, Some("prefix"))("table_1")
      with2.inputs.size should be(0)
      with2.actions(0).outputLabels should be(List("prefix_table_1"))
      with2.actions.size should be(1)
    }


  }

}
