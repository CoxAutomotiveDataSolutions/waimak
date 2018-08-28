package com.coxautodata.waimak

import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.log.Logging
import DFExecutorPriorityStrategies._

class ParallelDataFlowExecutor[C](val scheduler: ParallelActionScheduler[C],
                                  override val flowReporter: FlowReporter[C]
                                  , override val priorityStrategy: priorityStrategy[C])
  extends DataFlowExecutor[C] with Logging {

  override def initActionScheduler(): ActionScheduler[C] = scheduler

}

object ParallelDataFlowExecutor {

  def apply[C](flowReporter: FlowReporter[C])(poolIntoContext: (String, C) => Unit) = new ParallelDataFlowExecutor[C](ParallelActionScheduler()(poolIntoContext), flowReporter, defaultPriorityStrategy)

  def apply[C](flowReporter: FlowReporter[C], maxJobs: Int, priorityStrategy: priorityStrategy[C])(poolIntoContext: (String, C) => Unit) = new ParallelDataFlowExecutor[C](ParallelActionScheduler(maxJobs)(poolIntoContext), flowReporter, priorityStrategy)

  def apply[C](flowReporter: FlowReporter[C], poolsSpec: Map[String, Int], priorityStrategy: priorityStrategy[C])(poolIntoContext: (String, C) => Unit) = new ParallelDataFlowExecutor[C](ParallelActionScheduler(poolsSpec)(poolIntoContext), flowReporter, priorityStrategy)

}