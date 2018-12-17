package com.coxautodata.waimak.dataflow

trait FlowReporter {

  def reportActionStarted(action: DataFlowAction, flowContext: FlowContext): Unit

  def reportActionFinished(action: DataFlowAction, flowContext: FlowContext): Unit

}

class NoReportingFlowReporter extends FlowReporter {

  override def reportActionStarted(action: DataFlowAction, flowContext: FlowContext): Unit = Unit

  override def reportActionFinished(action: DataFlowAction, flowContext: FlowContext): Unit = Unit

}

object NoReportingFlowReporter {

  def apply(): NoReportingFlowReporter = new NoReportingFlowReporter()

}