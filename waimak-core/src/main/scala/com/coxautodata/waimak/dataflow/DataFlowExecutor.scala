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

    val executionResults: Try[(Seq[DataFlowAction[C]], DataFlow[C])] = loopExecution(preparedDataFlow, initActionScheduler(), Seq.empty)

    executionResults.get
  }

  /**
    * Used to report events on the flow.
    */
  def flowReporter: FlowReporter[C]

  def priorityStrategy: Seq[DataFlowAction[C]] => Seq[DataFlowAction[C]]

  def initActionScheduler(): ActionScheduler[C]

  /**
    * Execute the action by calling it's performAction function and unpack the result.
    *
    * @param action        Action to be performed
    * @param inputEntities Inputs for the actions
    * @param flowContext   Context of the dataflow
    * @return
    */
  def executeAction(action: DataFlowAction[C], inputEntities: DataFlowEntities, flowContext: C): ActionResult = {
    flowReporter.reportActionStarted(action, flowContext)
    action.performAction(inputEntities, flowContext) match {
      case Success(v) =>
        flowReporter.reportActionFinished(action, flowContext)
        v
      case Failure(e) => throw new DataFlowException(s"Exception performing action: ${action.logLabel}", e)
    }
  }

  @tailrec
  private def loopExecution(currentFlow: DataFlow[C]
                            , actionScheduler: ActionScheduler[C]
                            , successfulActions: Seq[DataFlowAction[C]]
                           ): Try[(Seq[DataFlowAction[C]], DataFlow[C])] = {
    val toSchedule: Option[(String, DataFlowAction[C])] = actionScheduler
      .availableExecutionPool()
      .flatMap(executionPoolName => priorityStrategy(actionScheduler.dropRunning(executionPoolName, currentFlow.nextRunnable(executionPoolName)))
        .headOption.map((executionPoolName, _))
      )
    toSchedule match {
      case None if (!actionScheduler.hasRunningActions()) => {
        logInfo(s"Flow exit successfulActions: ${successfulActions.mkString("[", "", "]")} remaining: ${currentFlow.actions.mkString("[", ",", "]")}")
        Success((successfulActions, currentFlow))
      }
      case None => {
        actionScheduler.waitToFinish() match {
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
        //submit action for execution
        flowReporter.reportActionStarted(action, currentFlow.flowContext)
        val inputEntities: DataFlowEntities = {
          currentFlow.inputs.filterLabels(action.inputLabels)
        }
        loopExecution(currentFlow, actionScheduler.submitAction(executionPoolName, action, inputEntities, currentFlow.flowContext), successfulActions)
      }
    }
  }

  /**
    *
    * @param actionResults
    * @param currentFlow
    * @param successfulActionsUntilNow
    * @return                   Success((new state of the flow, appended list of successful actions)), Failure will be returned
    *                           if at least one action in the actionResults has failed
    */
  private[dataflow] def processActionResults(actionResults: Seq[(DataFlowAction[C], Try[ActionResult])]
                                             , currentFlow: DataFlow[C]
                                             , successfulActionsUntilNow: Seq[DataFlowAction[C]]): Try[(DataFlow[C], Seq[DataFlowAction[C]])] = {
    val (success, failed) = actionResults.partition(_._2.isSuccess)
    val res = success.foldLeft( (currentFlow, successfulActionsUntilNow) ) { (res, actionRes) =>
      val action = actionRes._1
      flowReporter.reportActionFinished(action, currentFlow.flowContext)
      val nextFlow = res._1.executed(action, actionRes._2.get)
      (nextFlow, res._2 :+ action)
    }
    if (failed.isEmpty) {
      Success(res)
    } else {
      failed.foreach{ t =>
        // TODO: maybe add to flowReporter info about failed actions
        logError("Failed Action " + t._1.logLabel + " " + t._2.failed)
      }
      val failedAction = failed.head._1
      Failure(new DataFlowException(s"Exception performing action: ${failedAction.logLabel}", failed.head._2.failed.get)) //:face_palm:
    }
  }

}
