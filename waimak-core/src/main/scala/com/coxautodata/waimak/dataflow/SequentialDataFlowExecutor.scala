package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging

import scala.annotation.tailrec

/**
  * Created by Alexei Perelighin 2017/12/27
  *
  * Executes one action at a time wihtout trying to parallelize them.
  *
  * @tparam C the type of context which we pass to the actions
  */
class SequentialDataFlowExecutor[C](override val flowReporter: FlowReporter[C]
                                       , override val priorityStrategy: Seq[DataFlowAction[C]] => Seq[DataFlowAction[C]])
  extends DataFlowExecutor[C] with Logging {

  override def actionScheduler(): ActionScheduler[C] = new SequentialScheduler[C](None)
}


object SequentialDataFlowExecutor {

  def apply[C](flowReporter: FlowReporter[C]) = new SequentialDataFlowExecutor(flowReporter, identity[Seq[DataFlowAction[C]]])

}