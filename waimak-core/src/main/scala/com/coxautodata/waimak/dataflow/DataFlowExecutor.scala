package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by Alexei Perelighin on 11/01/18.
  */
trait DataFlowExecutor[C] extends Logging {

  /**
    * Executes as many actions as possible with the given DAG, stops when no more actions can be executed.
    *
    * @param dataFlow initial state with actions to execute and set inputs from previous actions
    * @return (Seq[EXECUTED ACTIONS], FINAL STATE). Final state does not contain the executed actions and the outputs
    *         of the executed actions are now in the inputs
    */
  def execute(dataFlow: DataFlow[C]): (Seq[DataFlowAction[C]], DataFlow[C]) = {
    val preparedDataFlow = dataFlow.prepareForExecution()

    val actionScheduler = initActionScheduler()
    val executionResults: Try[(Seq[DataFlowAction[C]], DataFlow[C])] = loopExecution(preparedDataFlow, actionScheduler, Seq.empty)

    actionScheduler.shutDown() match {
      case Failure(e) => logError("Problem shutting down execution pools", e)
      case _ => logInfo("Execution pools were shutdown ok.")
    }
    executionResults.get
  }

  
  /**
    * Used to report events on the flow.
    */
  def flowReporter: FlowReporter[C]

  /**
    * A complex data flow has lots of parallel, diverging and converging actions, lots of the actions could be started
    * in parallel, but certain actions if started earlier could lead to quicker end to end execution of all of the
    * flows and various strategies could lead to it. This strategy will always be applied to a set of actions to schedule
    * regardless of the scheduler implementation.
    *
    * @return
    */
  def priorityStrategy: DFExecutorPriorityStrategies.priorityStrategy[C]

  /**
    * Action scheduler used to run actions
    *
    * @return
    */
  def initActionScheduler(): ActionScheduler[C]

  @tailrec
  private def loopExecution(currentFlow: DataFlow[C]
                            , actionScheduler: ActionScheduler[C]
                            , successfulActions: Seq[DataFlowAction[C]]
                           ): Try[(Seq[DataFlowAction[C]], DataFlow[C])] = {
    toSchedule(currentFlow, actionScheduler) match {
      case None if !actionScheduler.hasRunningActions => //No more actions to schedule and none are running => finish data flow execution
        logInfo(s"Flow exit successfulActions: ${successfulActions.mkString("[", "", "]")} remaining: ${currentFlow.actions.mkString("[", ",", "]")}")
        Success((successfulActions, currentFlow))
      case None => {
        actionScheduler.waitToFinish(currentFlow.flowContext, flowReporter) match { // nothing to schedule, in order to continue need to wait for some running actions to finish to unlock other actions
          case Success((newScheduler, actionResults)) => {
            processActionResults(actionResults, currentFlow, successfulActions) match {
              case Success((newFlow, newSuccessfulActions)) => loopExecution(newFlow, newScheduler, newSuccessfulActions)
              case Failure(e) => Failure(e)
            }
          }
          case Failure(e) => Failure(e)
        }
      }
      case Some((executionPoolName, action)) => {
        //submit action for execution aka to schedule
        val inputEntities: DataFlowEntities = {
          currentFlow.inputs.filterLabels(action.inputLabels)
        }
        loopExecution(currentFlow, actionScheduler.schedule(executionPoolName, action, inputEntities, currentFlow.flowContext, flowReporter), successfulActions)
      }
    }
  }

  /**
    * Determines which execution pool to schedule in and an action to schedule into it.
    * Decision depends on:
    * 1) slots available in the pools
    * 2) actions available for the pools with slots
    * 3) priority strategy that will select and change the order of the available actions
    *
    * @param currentFlow
    * @param actionScheduler
    * @return (Pool into which to schedule, Action to schedule)
    */
  protected[dataflow] def toSchedule(currentFlow: DataFlow[C], actionScheduler: ActionScheduler[C]): Option[(String, DataFlowAction[C])] = { DEFAULT_POOL_NAME
    val toSchedule: Option[(String, DataFlowAction[C])] = actionScheduler
      .availableExecutionPools()
      .flatMap(executionPoolNames => priorityStrategy(actionScheduler.dropRunning(executionPoolNames, currentFlow.nextRunnable(executionPoolNames)))
        .headOption.map(actionToSchedule => (currentFlow.schedulingMeta.executionPoolName(actionToSchedule), actionToSchedule))
      )
    toSchedule
  }

  /**
    * Marks actions as processed in the data flow and if all were successful return new state of the data flow.
    *
    * @param actionResults Success or Failure of multiple actions
    * @param currentFlow   Flow in which to mark actions as successful
    * @param successfulActionsUntilNow
    * @return Success((new state of the flow, appended list of successful actions)), Failure will be returned
    *         if at least one action in the actionResults has failed
    */
  private[dataflow] def processActionResults(actionResults: Seq[(DataFlowAction[C], Try[ActionResult])]
                                             , currentFlow: DataFlow[C]
                                             , successfulActionsUntilNow: Seq[DataFlowAction[C]]): Try[(DataFlow[C], Seq[DataFlowAction[C]])] = {
    val (success, failed) = actionResults.partition(_._2.isSuccess)
    val res = success.foldLeft((currentFlow, successfulActionsUntilNow)) { (res, actionRes) =>
      val action = actionRes._1
      val nextFlow = res._1.executed(action, actionRes._2.get)
      (nextFlow, res._2 :+ action)
    }
    if (failed.isEmpty) {
      Success(res)
    } else {
      failed.foreach { t =>
        // TODO: maybe add to flowReporter info about failed actions
        logError("Failed Action " + t._1.logLabel + " " + t._2.failed)
      }
      failed.head._2.asInstanceOf[Try[(DataFlow[C], Seq[DataFlowAction[C]])]]
      Failure(throw new DataFlowException(s"Exception performing action: ${failed.head._1.logLabel}", failed.head._2.failed.get))
    }
  }

}
