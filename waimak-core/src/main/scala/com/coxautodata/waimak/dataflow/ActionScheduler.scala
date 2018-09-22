package com.coxautodata.waimak.dataflow

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
  def hasRunningActions: Boolean

  /**
    * Locks and waits for at least one action to finish running, can return more than one action if they have finished and
    * their results are available.
    *
    * @param flowContext
    * @param flowReporter
    * @return
    */
  def waitToFinish(flowContext: C, flowReporter: FlowReporter[C]): Try[(ActionScheduler[C], Seq[(DataFlowAction[C], Try[ActionResult])])]

  /**
    * Submits action into the specified execution pool.
    *
    * @param poolName
    * @param action
    * @param entities
    * @param flowContext
    * @param flowReporter
    * @return
    */
  def submitAction(poolName: String, action: DataFlowAction[C], entities: DataFlowEntities, flowContext: C, flowReporter: FlowReporter[C]): ActionScheduler[C]

  def shutDown(): Try[ActionScheduler[C]]

}