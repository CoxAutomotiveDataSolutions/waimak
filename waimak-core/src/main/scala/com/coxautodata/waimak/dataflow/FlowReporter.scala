package com.coxautodata.waimak.dataflow

trait FlowReporter[T, C] {

  def reportActionStarted(action: DataFlowAction[T, C], flowContext: C): Unit

  def reportActionFinished(action: DataFlowAction[T, C], flowContext: C): Unit

  def reportExecutionStarted(flow: DataFlow[T, C], executionGUID: String): Unit

  def reportExecutionFinished(flow: DataFlow[T, C], executionGUID: String): Unit

}

class NoReportingFlowReporter[T, C] extends FlowReporter[T, C] {
  override def reportActionStarted(action: DataFlowAction[T, C], flowContext: C): Unit = Unit

  override def reportActionFinished(action: DataFlowAction[T, C], flowContext: C): Unit = Unit

  override def reportExecutionStarted(flow: DataFlow[T, C], executionGUID: String): Unit = Unit

  override def reportExecutionFinished(flow: DataFlow[T, C], executionGUID: String): Unit = Unit
}

object NoReportingFlowReporter {
  def apply[T, C]: FlowReporter[T, C] = new NoReportingFlowReporter()
}