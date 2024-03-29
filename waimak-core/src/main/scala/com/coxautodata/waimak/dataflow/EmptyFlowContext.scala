package com.coxautodata.waimak.dataflow

import java.util.Properties


class EmptyFlowContext extends FlowContext {

  override def setPoolIntoContext(poolName: String): Unit = ()

  override def reportActionStarted(action: DataFlowAction): Unit = ()

  override def reportActionFinished(action: DataFlowAction): Unit = ()

  override def getOption(key: String): Option[String] = Option(conf.getProperty(key))

  val conf: Properties = new Properties()
}
