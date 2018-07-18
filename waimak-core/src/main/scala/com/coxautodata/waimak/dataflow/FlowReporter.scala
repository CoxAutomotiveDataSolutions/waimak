package com.coxautodata.waimak.dataflow

trait FlowReporter[C] {

  def reportActionStarted(action: DataFlowAction[C], flowContext: C): Unit

  def reportActionFinished(action: DataFlowAction[C], flowContext: C): Unit

  def reportExecutionStarted(flow: DataFlow[C], executionGUID: String): Unit

  def reportExecutionFinished(flow: DataFlow[C], executionGUID: String): Unit

}

class NoReportingFlowReporter[C] extends FlowReporter[C] {
  override def reportActionStarted(action: DataFlowAction[C], flowContext: C): Unit = Unit

  override def reportActionFinished(action: DataFlowAction[C], flowContext: C): Unit = Unit

  override def reportExecutionStarted(flow: DataFlow[C], executionGUID: String): Unit = Unit

  override def reportExecutionFinished(flow: DataFlow[C], executionGUID: String): Unit = Unit
}

object NoReportingFlowReporter {
  def apply[C]: FlowReporter[C] = new NoReportingFlowReporter()
}