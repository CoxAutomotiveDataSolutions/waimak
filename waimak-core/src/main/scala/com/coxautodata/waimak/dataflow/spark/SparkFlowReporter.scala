package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.{DataFlow, DataFlowAction, FlowReporter}
import org.apache.spark.{WaimakExecutionEvent, WaimakGraph}
import org.apache.spark.sql.Dataset
import org.apache.spark.ui.WaimakExecutionsUITab

object SparkFlowReporter extends FlowReporter[Dataset[_], SparkFlowContext] {
  override def reportActionStarted(action: DataFlowAction[Dataset[_], SparkFlowContext], flowContext: SparkFlowContext): Unit = {
    flowContext.spark.sparkContext.setJobGroup(action.guid, action.description)
  }

  override def reportActionFinished(action: DataFlowAction[Dataset[_], SparkFlowContext], flowContext: SparkFlowContext): Unit = {
    flowContext.spark.sparkContext.clearJobGroup()
  }

  override def reportExecutionStarted(flow: DataFlow[Dataset[_], SparkFlowContext], executionGUID: String): Unit = {
    // Create UI tab if it doesn't exist
    WaimakExecutionsUITab(flow.flowContext.spark.sparkContext)

    // Get data to pass to event
    val actions: Seq[String] = flow.actions.map(_.description)

    // Add flow as Event
    WaimakExecutionEvent.addEvent(flow.flowContext.spark.sparkContext, WaimakExecutionEvent(executionGUID, actions, WaimakGraph(flow)))
  }

  override def reportExecutionFinished(flow: DataFlow[Dataset[_], SparkFlowContext], executionGUID: String): Unit = Unit
}