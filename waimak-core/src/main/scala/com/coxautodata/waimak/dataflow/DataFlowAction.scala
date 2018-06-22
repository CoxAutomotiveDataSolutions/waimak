package com.coxautodata.waimak.dataflow

import java.util.UUID

import scala.util.Try

/**
  * An action to be performed as part of a data flow. Actions declare what input labels they expect in order to start
  * execution (0 .. N) and can produce outputs associated with the output labels (0 .. N). Executors will use these labels
  * to execute to schedule the actions sequentially or in parallel.
  *
  * @tparam T the type of the entity which we are transforming (e.g. Dataset
  * @tparam C the type of the context of the flow in which this action runs
  */
trait DataFlowAction[T, C] {

  /**
    * This action can only be executed if all of the inputs are not empty. An input can be explicitly marked as empty.
    * If false, than one or more inputs can be empty to start execution.
    *
    * @return
    */
  def requiresAllInputs: Boolean = true

  /**
    * The unique identifiers for the inputs to this action
    */
  val inputLabels: List[String]

  /**
    * The unique identifiers for the outputs to this action
    */
  val outputLabels: List[String]

  val guid: String = UUID.randomUUID().toString

  /**
    * For representing the action
    */
  def actionName: String = getClass.getSimpleName

  def description = s"Action: $actionName Inputs: ${inputLabels.mkString("[", ",", "]")} Outputs: ${outputLabels.mkString("[", ",", "]")}"

  def logLabel = s"$guid: $description"

  /**
    * Perform the action
    *
    * @param inputs      the [[DataFlowEntities]] corresponding to the [[inputLabels]]
    * @param flowContext context of the flow in which this action runs
    * @return the action outputs (these must be declared in the same order as their labels in [[outputLabels]])
    */
  def performAction(inputs: DataFlowEntities[T], flowContext: C): Try[ActionResult[T]]

  /**
    * Action has the responsibility of assessing itself and produce DataFlowActionState, that will be used by the
    * executors to determine if they can call performAction or not. Also can be used for progress monitoring.
    * This will allow for more custom actions without modifying the executors
    *
    * @param inputs - action will study the state of the inputs in order to generate self assessment
    * @return - an instance of the DataFlowActionState
    */
  def flowState(inputs: DataFlowEntities[Option[T]]): DataFlowActionState = {
    if (inputLabels.isEmpty) ReadyToRun(Seq.empty)
    else {
      val in = inputLabels.map(label => (label, inputs.entities.get(label) match {
        case None => 0 // no input
        case Some(None) => 1 // input was produced, but is empty
        case Some(_) => 2 // input is not empty
      }))
      val missing = in.filter(_._2 == 0).map(_._1)
      val emptyInputs = in.filter(_._2 == 1).map(_._1)
      val res = (missing.isEmpty, requiresAllInputs, emptyInputs.isEmpty) match {
        case (false, _, _) => RequiresInput(inputLabels.filter(!missing.contains(_)), missing)
        case (true, true, false) => ExpectedInputIsEmpty(inputLabels.filter(!emptyInputs.contains(_)), emptyInputs)
        case (true, true, true) => ReadyToRun(inputLabels)
        case (true, false, _) => ReadyToRun(inputLabels)
        case _ => throw new DataFlowException(s"Can not guess in which state the action is. ${logLabel}")
      }
      res
    }
  }
}

/**
  * State of the action.
  */
trait DataFlowActionState {

  /**
    * List of action's input labels that the action expects and is present in the inputs of the flow.
    *
    * @return
    */
  def ready: Seq[String]

  /**
    * List of action's input labels that the action expects and is not present in the inputs of the flow.
    *
    * @return
    */
  def notReady: Seq[String]

}

/**
  * Action can not be executed as it requires more inputs to be available.
  *
  * @param ready
  * @param notReady
  */
sealed case class RequiresInput(ready: Seq[String], notReady: Seq[String]) extends DataFlowActionState

/**
  * Can not be executed, as expected input is present, but is empty
  *
  * @param ready
  * @param notReady
  */
sealed case class ExpectedInputIsEmpty(ready: Seq[String], notReady: Seq[String]) extends DataFlowActionState

/**
  * Action is ready to run.
  *
  * @param ready
  */
sealed case class ReadyToRun(ready: Seq[String]) extends DataFlowActionState {
  val notReady: Seq[String] = Seq.empty
}

/**
  * Action was executed and can not be executed again.
  */
sealed case class Executed() extends DataFlowActionState {

  val ready: Seq[String] = Seq.empty

  val notReady: Seq[String] = Seq.empty

  def readToRun: Boolean = false

}