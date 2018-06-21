package com.coxautodata.waimak.dataflow

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

}
