package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging

import scala.util.{Failure, Success, Try}

/**
  * Defines functions that are specific to scheduling tasks, evaluating which execution pools are available and
  * signaling back which actions have finished their execution.
  *
  * Created by Alexei Perelighin on 2018/07/06
  */
trait ActionScheduler[C] {

  /**
    * finds execution pools that have slots to run actions.
    *
    * @return None if none of the pools are available for scheduling and Some(Set[AVAILABLE POOL NAME]) - all pools
    *         that are available for scheduling
    */
  def availableExecutionPools(): Option[Set[String]]

  /**
    * Removes actions that are already running in the specified set pools.
    *
    * @param poolNames
    * @param from
    * @return
    */
  def dropRunning(poolNames: Set[String], from: Seq[DataFlowAction[C]]): Seq[DataFlowAction[C]]

  /**
    * Checks if there are actions running at all, regardless of the execution pool.
    *
    * @return
    */
  def hasRunningActions(): Boolean

  /**
    * Locks and waits for at least one action to finish running, can return more than one action if they have finished and
    * their results are available.
    *
    * @return
    */
  def waitToFinish(): Try[ (ActionScheduler[C], Seq[(DataFlowAction[C], Try[ActionResult])]) ]

  /**
    * Submits action into the specified execution pool.
    *
    * @param poolName
    * @param action
    * @param entities
    * @param flowContext
    * @return
    */
  def submitAction(poolName: String, action: DataFlowAction[C], entities: DataFlowEntities, flowContext: C): ActionScheduler[C]

}

/**
  * Executes only one action at a time.
  *
  * @param toRun
  * @tparam C
  */
class SequentialScheduler[C](val toRun: Option[(DataFlowAction[C], DataFlowEntities, C)])
  extends ActionScheduler[C] with Logging {

  override def availableExecutionPools(): Option[Set[String]] = {
    logDebug("canSchedule " + toRun)
    if (toRun.isDefined) None else Some(Set(DEFAULT_POOL_NAME))
  }

  override def dropRunning(poolNames: Set[String], from: Seq[DataFlowAction[C]]): Seq[DataFlowAction[C]] = toRun.fold(from)(r => from.filterNot(_.guid == r._1.guid))

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

  override def submitAction(poolName: String, action: DataFlowAction[C], entities: DataFlowEntities, flowContext: C): ActionScheduler[C] = {
    logInfo("submitAction " + action.logLabel)
    new SequentialScheduler[C](Some((action, entities, flowContext)) )
  }

}