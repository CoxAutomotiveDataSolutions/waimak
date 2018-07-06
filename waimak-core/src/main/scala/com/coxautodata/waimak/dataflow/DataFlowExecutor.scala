package com.coxautodata.waimak.dataflow

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by Alexei Perelighin on 11/01/18.
  */
trait DataFlowExecutor[C] {

  /**
    * Executes as many actions as possible with the given DAG, stops when no more actions can be executed.
    *
    * @param dataFlow initial state with actions to execute and set inputs from previous actions
    * @return (Seq[EXECUTED ACTIONS], FINAL STATE). Final state does not contain the executed actions and the outputs
    *         of the executed actions are now in the inputs
    */
  def execute(dataFlow: DataFlow[C]): (Seq[DataFlowAction[C]], DataFlow[C]) = {
    val preparedDataFlow = dataFlow.prepareForExecution()

    val executionResults: Try[(Seq[DataFlowAction[C]], DataFlow[C])] = loopExecution(None, preparedDataFlow, initActionScheduler(), Seq.empty)

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
  private def loopExecution(resultsToProcess: Option[ Seq[(DataFlowAction[C], Try[ActionResult])] ]
                   , currentFlow: DataFlow[C]
                    , actionScheduler: ActionScheduler[C]
                    , successfulActions: Seq[DataFlowAction[C]]
                   ): Try[(Seq[DataFlowAction[C]], DataFlow[C])] = {
    //action scheduling and processing of the results are done in different loopExecution calls, this has simplified
    //implementation of the state machine.
    resultsToProcess match {
      case Some(actionResults) => {
        //process the results of the action execution
        val (success, failed) = actionResults.partition(_._2.isSuccess)
        val (newFlow, newSuccessfulActions) = success.foldLeft( (currentFlow, successfulActions) ) { (res, actionRes) =>
          val action = actionRes._1
          flowReporter.reportActionFinished(action, currentFlow.flowContext)
          val nextFlow = res._1.executed(action, actionRes._2.get)
          (nextFlow, res._2 :+ action)
        }
        if (failed.isEmpty) {
          // continue with scheduling
          loopExecution(None, newFlow, actionScheduler, newSuccessfulActions)
        } else {
          failed.foreach{ t =>
//            logErrro(t)
            // TODO: maybe add to flowReporter info about failed actions
            println("DEBUG failed " + t._1.logLabel + " " + t._2.failed)
          }
          val failedAction = failed.head._1
          //
          Failure(new DataFlowException(s"Exception performing action: ${failedAction.logLabel}", failed.head._2.failed.get)) //:face_palm:
        }
      }
      case None => {
        if (actionScheduler.canSchedule()) {
          val toSchedule: Option[DataFlowAction[C]] = priorityStrategy(actionScheduler.dropRunning(currentFlow.nextRunnable())).headOption
          toSchedule match {
            case None if (!actionScheduler.hasRunningActions()) => {
//              logInfo("Finished Flow" + successfulActions)
              println(s"DEBUG Flow exit successfulActions: ${successfulActions.mkString("[","", "]")} remaining: ${currentFlow.actions.mkString("[", ",", "]")}")
              Success((successfulActions, currentFlow))
            }
            case None => {
              actionScheduler.waitToFinish() match {
                case Success((newScheduler, actionResults)) => loopExecution(Some(actionResults), currentFlow, newScheduler, successfulActions)
                case Failure(e) => Failure(e)
              }
            }
            case Some(action) => {
              //submit action for execution
              flowReporter.reportActionStarted(action, currentFlow.flowContext)
              val inputEntities: DataFlowEntities = {
                currentFlow.inputs.filterLabels(action.inputLabels)
              }
              loopExecution(None, currentFlow, actionScheduler.submitAction(action, inputEntities, currentFlow.flowContext), successfulActions)
            }
          }
        } else {
          actionScheduler.waitToFinish() match {
            case Success((newScheduler, actionResults)) => loopExecution(Some(actionResults), currentFlow, newScheduler, successfulActions)
            case Failure(e) => Failure(e)
          }
        }
      }
    }
  }

}
