package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging

import scala.annotation.tailrec

/**
  * Created by Alexei Perelighin 2017/12/27
  *
  * Executes one action at a time wihtout trying to parallelize them.
  */
class SequentialDataFlowExecutor(override val flowReporter: FlowReporter
                                       , override val priorityStrategy: Seq[DataFlowAction] => Seq[DataFlowAction])
  extends DataFlowExecutor with Logging {

  /**
    * Action scheduler used to run actions
    *
    * @return
    */
  override def initActionScheduler(): ActionScheduler = new SequentialScheduler(None)
}


object SequentialDataFlowExecutor {

  def apply(flowReporter: FlowReporter) = new SequentialDataFlowExecutor(flowReporter, identity[Seq[DataFlowAction]])

}