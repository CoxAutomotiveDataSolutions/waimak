package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.spark.TestSparkData.{basePath, persons, purchases}
import com.coxautodata.waimak.dataflow.{DFExecutorPriorityStrategies, DataFlowException, Waimak}
import org.apache.spark.sql.{DataFrameReader, Dataset}

class TestFlowCombinators extends SparkAndTmpDirSpec {
  override val appName: String = "Combinators test"

  val executor = Waimak.sparkExecutor(1, DFExecutorPriorityStrategies.preferLoaders)

  it("should combine a simple flow correctly") {
    val emptyFlow = Waimak.sparkFlow(sparkSession)
    val with2 = emptyFlow.openTable("test")("table_1", "table_2")
    with2.inputs.size should be(0)
    with2.actions(0).outputLabels should be(List("table_1", "table_2"))
    with2.actions.size should be(1)

    val with3 = emptyFlow.openTable("test")("table_3", "table_4", "table_5")
    with3.inputs.size should be(0)
    with3.actions(0).outputLabels should be(List("table_3", "table_4", "table_5"))
    with3.actions.size should be(1)

    val combine = with2 ++ with3

    combine.inputs.size should be(0)
    combine.actions(0).outputLabels should be(List("table_1", "table_2"))
    combine.actions(1).outputLabels should be(List("table_3", "table_4", "table_5"))
    combine.actions.size should be(2)
  }

  it("should run a flow combined correctly") {
    val spark = sparkSession
    import spark.implicits._

    val flow1 = Waimak.sparkFlow(spark)
      .openCSV(basePath)("csv_1")
      .show("csv_1")

    val flow2 = Waimak.sparkFlow(spark)
      .openCSV(basePath)("csv_2")
      .show("csv_2")

    val flow = flow1 ++ flow2

    val (executedActions, finalState) = executor.execute(flow)

    //validate executed actions
    executedActions.size should be(4)
    executedActions.map(a => a.description) should be(Seq("Action: read Inputs: [] Outputs: [csv_1]", "Action: read Inputs: [] Outputs: [csv_2]", "Action: show Inputs: [csv_1] Outputs: []", "Action: show Inputs: [csv_2] Outputs: []"))

    finalState.actions.size should be(0) // no actions to execute
    finalState.inputs.size should be(2)
    finalState.inputs.getOption[Dataset[_]]("csv_1").map(_.as[TPurchase].collect()).get should be(purchases)
    finalState.inputs.getOption[Dataset[_]]("csv_2").map(_.as[TPerson].collect()).get should be(persons)
  }

  it("should fail when there are duplicate labels in the flow") {
    val spark = sparkSession

    val flow1 = Waimak.sparkFlow(spark)
      .openCSV(basePath)("csv_1")
      .show("csv_1")

    val flow2 = Waimak.sparkFlow(spark)
      .openCSV(basePath)("csv_1")
      .show("csv_1")

    an[DataFlowException] should be thrownBy (flow1 ++ flow2)
  }

  it("should work with multiple combine operations") {
    val spark = sparkSession
    import spark.implicits._

    val flow1 = Waimak.sparkFlow(spark)
      .openCSV(basePath)("csv_1")
      .show("csv_1")

    val flow2 = Waimak.sparkFlow(spark)
      .openCSV(basePath)("csv_2")
      .show("csv_2")

    val testNames = Seq("one", "two", "three", "four", "five")

    val testFlows = testNames.map(n => {
      Waimak.sparkFlow(spark).open(n, (_: SparkFlowContext) => Seq("hi", "world").toDS())
    })

    val combinedFlow = SparkDataFlow.combine(flow1, flow2, testFlows: _*)

    executor.execute(combinedFlow)

    println("Combined flow actions")
    println(
      combinedFlow.actions
        .map(a => s"Inputs: [${a.inputLabels.toString}] | Outputs: [${a.outputLabels.toString}]" + System.lineSeparator())
    )
  }
}
