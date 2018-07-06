package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging

import scala.util.{Failure, Success, Try}

/**
  * Created by Alexei Perelighin on 2018/07/06
  */
trait ActionScheduler[C] {

  def canSchedule(): Boolean

  def dropRunning(from: Seq[DataFlowAction[C]]): Seq[DataFlowAction[C]]

  def hasRunningActions(): Boolean

  def waitToFinish(): Try[ (ActionScheduler[C], Seq[(DataFlowAction[C], Try[ActionResult])]) ]

  def submitAction(action: DataFlowAction[C], entities: DataFlowEntities, flowContext: C): ActionScheduler[C]

}

class SequentialScheduler[C](val toRun: Option[(DataFlowAction[C], DataFlowEntities, C)])
  extends ActionScheduler[C] with Logging {

  override def canSchedule(): Boolean = {
    logDebug("canSchedule " + toRun)
    !toRun.isDefined
  }

  override def dropRunning(from: Seq[DataFlowAction[C]]): Seq[DataFlowAction[C]] = toRun.fold(from)(r => from.filterNot(_.guid == r._1.guid))

  override def hasRunningActions(): Boolean = toRun.isDefined

  override def waitToFinish(): Try[(ActionScheduler[C], Seq[(DataFlowAction[C], Try[ActionResult])])] = {
    logInfo("waitToFinish " + toRun.fold("None")(e => e._1.logLabel))
    toRun match {
      case Some((action, entities, context)) => {
        val actionRes = Seq((action, action.performAction(entities, context)))
        Success((new SequentialScheduler(None), actionRes))
      }
      case None => Failure(new RuntimeException("Error while waiting to finish"))
    }
  }

  override def submitAction(action: DataFlowAction[C], entities: DataFlowEntities, flowContext: C): ActionScheduler[C] = {
    logInfo("submitAction " + action.logLabel)
    new SequentialScheduler[C](Some((action, entities, flowContext)) )
  }

}