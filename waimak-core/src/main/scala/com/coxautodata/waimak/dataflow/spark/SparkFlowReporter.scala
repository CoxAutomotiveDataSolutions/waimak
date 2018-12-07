package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.{DataFlowAction, FlowContext, FlowReporter}

object SparkFlowReporter extends FlowReporter {

  override def reportActionStarted(action: DataFlowAction, flowContext: FlowContext): Unit = {
    flowContext.reportActionStarted(action)

  }

  override def reportActionFinished(action: DataFlowAction, flowContext: FlowContext): Unit = {
    flowContext.reportActionFinished(action)
  }

}