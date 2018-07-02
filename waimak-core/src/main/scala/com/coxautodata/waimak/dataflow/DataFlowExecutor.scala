package com.coxautodata.waimak.dataflow

import scala.util.{Failure, Success}

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
  def execute(dataFlow: DataFlow[T, C]): (Seq[DataFlowAction[T, C]], DataFlow[T, C])

  /**
    * Used to report events on the flow.
    */
  def flowReporter: FlowReporter[T, C]

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

}
