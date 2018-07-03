package com.coxautodata.waimak.dataflow

trait FlowReporter[C] {

  def reportActionStarted(action: DataFlowAction[C], flowContext: C): Unit

  def reportActionFinished(action: DataFlowAction[C], flowContext: C): Unit

}

class NoReportingFlowReporter[C] extends FlowReporter[C] {
  override def reportActionStarted(action: DataFlowAction[C], flowContext: C): Unit = Unit

  override def reportActionFinished(action: DataFlowAction[C], flowContext: C): Unit = Unit
}

object NoReportingFlowReporter {
  def apply[C]: FlowReporter[C] = new NoReportingFlowReporter()
}