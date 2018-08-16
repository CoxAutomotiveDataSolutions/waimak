package com.coxautodata.waimak

import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.log.Logging

class ParallelDataFlowExecutor[C](val scheduler: ParallelActionScheduler[C],
                                  override val flowReporter: FlowReporter[C]
                                  , override val priorityStrategy: Seq[DataFlowAction[C]] => Seq[DataFlowAction[C]])
  extends DataFlowExecutor[C] with Logging {

  override def initActionScheduler(): ActionScheduler[C] = scheduler

}

object ParallelDataFlowExecutor {

  def apply[C](flowReporter: FlowReporter[C]) = new ParallelDataFlowExecutor[C](ParallelActionScheduler(), flowReporter, identity[Seq[DataFlowAction[C]]])

}