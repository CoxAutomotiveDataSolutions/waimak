package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.dataflow.DFExecutorPriorityStrategies._
import com.coxautodata.waimak.log.Logging

class ParallelDataFlowExecutor(val scheduler: ParallelActionScheduler
                                  , override val flowReporter: FlowReporter
                                  , override val priorityStrategy: priorityStrategy)
  extends DataFlowExecutor with Logging {

  /**
    * Action scheduler used to run actions
    *
    * @return
    */
  override def initActionScheduler(): ActionScheduler = scheduler
}

object ParallelDataFlowExecutor {

  def apply(flowReporter: FlowReporter) = new ParallelDataFlowExecutor(ParallelActionScheduler(), flowReporter, defaultPriorityStrategy)

  def apply(flowReporter: FlowReporter, maxJobs: Int, priorityStrategy: priorityStrategy) = new ParallelDataFlowExecutor(ParallelActionScheduler(maxJobs), flowReporter, priorityStrategy)

  def apply(flowReporter: FlowReporter, poolsSpec: Map[String, Int], priorityStrategy: priorityStrategy) = new ParallelDataFlowExecutor(ParallelActionScheduler(poolsSpec), flowReporter, priorityStrategy)

}