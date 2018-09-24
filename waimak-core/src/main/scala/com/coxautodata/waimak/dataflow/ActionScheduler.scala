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
    * @param poolNames  pool names to which the from actions belong to
    * @param from       list of actions from poolNames that DataFlow knows have not been marked as executed and can be scheduled
    * @return           list of action the do not contain running actions
    */
  def dropRunning(poolNames: Set[String], from: Seq[DataFlowAction[C]]): Seq[DataFlowAction[C]]

  /**
    * Checks if there are actions running at all, regardless of the execution pool.
    *
    * @return true if at least one action is running in any of the pools
    */
  def hasRunningActions: Boolean

  /**
    * Locks and waits for at least one action to finish running, can return more than one action if they have finished and
    * their results are available.
    *
    * @param flowContext   object that allows access to the context of the flow and application
    * @param flowReporter  object that is used to signal start and end of the action execution
    * @return
    */
  def waitToFinish(flowContext: C, flowReporter: FlowReporter[C]): Try[(ActionScheduler[C], Seq[(DataFlowAction[C], Try[ActionResult])])]

  /**
    * Submits action into the specified execution pool.
    *
    * @param poolName     pool into which to schedule the action
    * @param action       action to schedule
    * @param entities     action labels that have data
    * @param flowContext  object that allows access to the context of the flow and application
    * @param flowReporter object that is used to signal start and end of the action execution
    * @return
    */
  def schedule(poolName: String, action: DataFlowAction[C], entities: DataFlowEntities, flowContext: C, flowReporter: FlowReporter[C]): ActionScheduler[C]

  /**
    * Executors must call it before exiting the execuiton of the flow to release resources.
    *
    * @return
    */
  def shutDown(): Try[ActionScheduler[C]]

}