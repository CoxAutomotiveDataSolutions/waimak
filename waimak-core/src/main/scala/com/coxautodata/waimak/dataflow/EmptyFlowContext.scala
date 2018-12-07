package com.coxautodata.waimak.dataflow

class EmptyFlowContext extends FlowContext {

  override def setPoolIntoContext(poolName: String): Unit = Unit

  override def reportActionStarted(action: DataFlowAction): Unit = Unit

  override def reportActionFinished(action: DataFlowAction): Unit = Unit
  
}
