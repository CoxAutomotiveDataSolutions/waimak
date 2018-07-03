package com.coxautodata.waimak.dataflow

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by Alexei Perelighin on 11/01/18.
  */
trait DataFlowExecutor[T, C] {

  /**
    * Executes as many actions as possible with the given DAG, stops when no more actions can be executed.
    *
    * @param dataFlow initial state with actions to execute and set inputs from previous actions
    * @return (Seq[EXECUTED ACTIONS], FINAL STATE). Final state does not contain the executed actions and the outputs
    *         of the executed actions are now in the inputs
    */
  def execute(dataFlow: DataFlow[T, C]): (Seq[DataFlowAction[T, C]], DataFlow[T, C]) = {
    val preparedDataFlow = dataFlow.prepareForExecution()

    val executionResults: Try[(Seq[DataFlowAction[T, C]], DataFlow[T, C])] = loopExecution(None, preparedDataFlow, Set.empty, Seq.empty)

    executionResults.get
  }

  /**
    * Used to report events on the flow.
    */
  def flowReporter: FlowReporter[T, C]

  def priorityStrategy: Seq[DataFlowAction[T, C]] => Seq[DataFlowAction[T, C]]

  /**
    * Execute the action by calling it's performAction function and unpack the result.
    *
    * @param action        Action to be performed
    * @param inputEntities Inputs for the actions
    * @param flowContext   Context of the dataflow
    * @return
    */
  def executeAction(action: DataFlowAction[T, C], inputEntities: DataFlowEntities[T], flowContext: C): Seq[Option[T]] = {
    flowReporter.reportActionStarted(action, flowContext)
    action.performAction(inputEntities, flowContext) match {
      case Success(v) =>
        flowReporter.reportActionFinished(action, flowContext)
        v
      case Failure(e) => throw new DataFlowException(s"Exception performing action: ${action.logLabel}", e)
    }
  }

  @tailrec
  private def loopExecution(resultsToProcess: Option[ Try[Seq[(DataFlowAction[T, C], Try[ActionResult[T]])]] ]
                   , currentFlow: DataFlow[T, C]
                    , running: Set[String]
                    , successfulActions: Seq[DataFlowAction[T, C]]
                   ): Try[(Seq[DataFlowAction[T, C]], DataFlow[T, C])] = {
    //action scheduling and processing of the results are done in different loopExecution calls, this has simplified
    //implementation of the state machine.
    resultsToProcess match {
      case Some(Failure(e)) => Failure(e)
      case Some(Success(actionResults)) => {
        //process the results of the action execution
        val (success, failed) = actionResults.partition(_._2.isSuccess)
        val (newFlow, newRunning, newSuccessfulActions) = success.foldLeft( (currentFlow, running, successfulActions) ) { (res, actionRes) =>
          val action = actionRes._1
          flowReporter.reportActionFinished(action, currentFlow.flowContext)
          val nextFlow = res._1.executed(action, actionRes._2.get)
          (nextFlow, res._2.filterNot(_ == action.guid), res._3 :+ action)
        }
        if (failed.isEmpty) {
          // continue with scheduling
          loopExecution(None, newFlow, newRunning, newSuccessfulActions)
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
        if (canSchedule(running)) {
          val toSchedule: Option[DataFlowAction[T, C]] = priorityStrategy(currentFlow.nextRunnable().filter(a => !running.contains(a.guid))).headOption
          toSchedule match {
            case None if (running.isEmpty) => {
//              logInfo("Finished Flow" + successfulActions)
              println(s"DEBUG Flow exit successfulActions: ${successfulActions.mkString("[","", "]")} remaining: ${currentFlow.actions.mkString("[", ",", "]")}")
              Success((successfulActions, currentFlow))
            }
            case None => loopExecution(Some(waitToFinish()), currentFlow, running, successfulActions)
            case Some(action) => {
              //submit action for execution
              flowReporter.reportActionStarted(action, currentFlow.flowContext)
              val inputEntities: DataFlowEntities[T] = {
                if (action.requiresAllInputs) currentFlow.inputs.filterLabels(action.inputLabels).map(_.get)
                else currentFlow.inputs.filterLabels(action.inputLabels).filterValues(_.isDefined).map(_.get)
              }
              submitAction(action, inputEntities, currentFlow.flowContext)
              loopExecution(None, currentFlow, running + action.guid, successfulActions)
            }
          }
        } else {
          loopExecution(Some(waitToFinish()), currentFlow, running, successfulActions)
        }
      }
    }
  }

  private var toRun: Option[(DataFlowAction[T, C], DataFlowEntities[T], C)] = None

  /**
    * Decides if more actions can be scheduled.
    * In case of sequential exec it is running.isEmpty
    * @param running
    * @return
    */
  def canSchedule(running: Set[String]): Boolean = {
    println("DEBUG canSchedule " + running.mkString("[", ",", "]"))
    running.isEmpty
  }

  /**
    * Collects the results of the multiple action executions.
    * In case of sequential exec it calls DataFlowAction.performAction
    * @return
    */
  def waitToFinish(): Try[Seq[(DataFlowAction[T, C], Try[ActionResult[T]])]] = {
    println("DEBUG waitToFinish " + toRun.fold("None")(e => e._1.logLabel))
    toRun match {
      case Some((action, entities, context)) => Success(Seq((action, action.performAction(entities, context))))
      case None => Failure(new RuntimeException("Error while waiting to finish"))
    }
  }

  /**
    * Submits
    * @param action
    */
  def submitAction(action: DataFlowAction[T, C], entities: DataFlowEntities[T], flowContext: C): Unit = {
    println("DEBUG submitAction " + action.logLabel)
    toRun = Some((action, entities, flowContext))
  }


}
