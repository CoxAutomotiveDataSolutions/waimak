package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.{DataFlowAction, FlowContext, FlowReporter}

object SparkFlowReporter extends FlowReporter {

  override def reportActionStarted(action: DataFlowAction, flowContext: FlowContext): Unit = {
    flowContext.asInstanceOf[SparkFlowContext].spark.sparkContext.setJobGroup(action.guid, action.description)

  }

  override def reportActionFinished(action: DataFlowAction, flowContext: FlowContext): Unit = {
    flowContext.asInstanceOf[SparkFlowContext].spark.sparkContext.clearJobGroup()
  }
}