package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging

import scala.util.{Failure, Success, Try}

/**
  * Executes only one action at a time.
  *
  * Created by Alexei Perelighin on 2018/07/06
  *
  * @param toRun  scheduled action. If None, than nothing is scheduled
  */
class SequentialScheduler(val toRun: Option[(DataFlowAction, DataFlowEntities, FlowContext)])
  extends ActionScheduler with Logging {

  override def availableExecutionPools: Option[Set[String]] = {
    logDebug("canSchedule " + toRun)
    if (toRun.isDefined) None else Some(Set(DEFAULT_POOL_NAME))
  }

  override def dropRunning(poolNames: Set[String], from: Seq[DataFlowAction]): Seq[DataFlowAction] = toRun.fold(from)(r => from.filterNot(_.guid == r._1.guid))

  override def hasRunningActions: Boolean = toRun.isDefined

  override def waitToFinish(flowContext: FlowContext, flowReporter: FlowReporter): (ActionScheduler, Seq[(DataFlowAction, Try[ActionResult])]) = {
    logInfo("waitToFinish " + toRun.fold("None")(e => e._1.logLabel))
    toRun match {
      case Some((action, entities, context)) => {
        flowReporter.reportActionStarted(action, flowContext)
        val actionRes = Seq((action, Try(action.performAction(entities, context)).flatten))
        flowReporter.reportActionFinished(action, flowContext)
        (new SequentialScheduler(None), actionRes)
      }
      case None => throw new RuntimeException("Called waitToFinish when there is nothing to run")
    }
  }

  override def schedule(poolName: String, action: DataFlowAction, entities: DataFlowEntities, flowContext: FlowContext, flowReporter: FlowReporter): ActionScheduler = {
    logInfo("schedule action " + action.logLabel)
    new SequentialScheduler(Some((action, entities, flowContext)))
  }

  override def shutDown(): Try[ActionScheduler] = Success(this)

}
