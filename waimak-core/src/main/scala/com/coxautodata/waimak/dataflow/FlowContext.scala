package com.coxautodata.waimak.dataflow

trait FlowContext {

  def setPoolIntoContext(poolName: String): Unit

  def reportActionStarted(action: DataFlowAction): Unit

  def reportActionFinished(action: DataFlowAction): Unit

}
