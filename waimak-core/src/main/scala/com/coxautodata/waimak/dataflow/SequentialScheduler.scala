package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging

import scala.util.{Failure, Success, Try}

/**
  * Executes only one action at a time.
  *
  * Created by Alexei Perelighin on 2018/07/06
  *
  * @param toRun  scheduled action. If None, than nothing is scheduled
  * @tparam C
  */
class SequentialScheduler[C](val toRun: Option[(DataFlowAction[C], DataFlowEntities, C)])
  extends ActionScheduler[C] with Logging {

  override def availableExecutionPools(): Option[Set[String]] = {
    logDebug("canSchedule " + toRun)
    if (toRun.isDefined) None else Some(Set(DEFAULT_POOL_NAME))
  }

  override def dropRunning(poolNames: Set[String], from: Seq[DataFlowAction[C]]): Seq[DataFlowAction[C]] = toRun.fold(from)(r => from.filterNot(_.guid == r._1.guid))

  override def hasRunningActions: Boolean = toRun.isDefined

  override def waitToFinish(flowContext: C, flowReporter: FlowReporter[C]): Try[(ActionScheduler[C], Seq[(DataFlowAction[C], Try[ActionResult])])] = {
    logInfo("waitToFinish " + toRun.fold("None")(e => e._1.logLabel))
    toRun match {
      case Some((action, entities, context)) => {
        flowReporter.reportActionStarted(action, flowContext)
        val actionRes = Seq((action, Try(action.performAction(entities, context)).flatten))
        flowReporter.reportActionFinished(action, flowContext)
        Success((new SequentialScheduler(None), actionRes))
      }
      case None => Failure(new RuntimeException("Error while waiting to finish"))
    }
  }

  override def schedule(poolName: String, action: DataFlowAction[C], entities: DataFlowEntities, flowContext: C, flowReporter: FlowReporter[C]): ActionScheduler[C] = {
    logInfo("schedule action " + action.logLabel)
    new SequentialScheduler[C](Some((action, entities, flowContext)))
  }

  override def shutDown(): Try[ActionScheduler[C]] = Success(this)

}
