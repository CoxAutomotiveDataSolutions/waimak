package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.spark.ui.WaimakGraph
import com.coxautodata.waimak.dataflow.{DataFlow, DataFlowAction, FlowReporter}
import org.apache.spark.ui.{WaimakExecutionEvent, WaimakExecutionsUITab}

object SparkFlowReporter extends FlowReporter[SparkFlowContext] {
  override def reportActionStarted(action: DataFlowAction[SparkFlowContext], flowContext: SparkFlowContext): Unit = {
    flowContext.spark.sparkContext.setJobGroup(action.guid, action.description)
  }

  override def reportActionFinished(action: DataFlowAction[SparkFlowContext], flowContext: SparkFlowContext): Unit = {
    flowContext.spark.sparkContext.clearJobGroup()
  }

  override def reportExecutionStarted(flow: DataFlow[SparkFlowContext], executionGUID: String): Unit = {
    // Create UI tab if it doesn't exist
    WaimakExecutionsUITab(flow.flowContext.spark.sparkContext)

    // Get data to pass to event
    val actions: Seq[String] = flow.actions.map(_.description)

    // Add flow as Event
    WaimakExecutionEvent.addEvent(flow.flowContext.spark.sparkContext, WaimakExecutionEvent(executionGUID, actions, WaimakGraph(flow)))
  }

  override def reportExecutionFinished(flow: DataFlow[SparkFlowContext], executionGUID: String): Unit = Unit
}