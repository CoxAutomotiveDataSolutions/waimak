package com.coxautodata.waimak.dataflow

import java.util.UUID

import scala.util.Try


/**
  * This action can be added to the flow over an existing action, it will be scheduled instead of it and can override or
  * intercept the behaviour of an action.
  * This is useful when additional behaviours need to be added. Examples: register as spark temp views for sql, logging
  * filtering, persisting to disk, dredging, etc.
  *
  * Created by Alexei Perelighin on 23/02/2018.
  *
  * @tparam C the type of the context of the flow in which this action runs
  */
class InterceptorAction(val intercepted: DataFlowAction) extends DataFlowAction {

  /**
    * @return same value as the intercepted action.
    */
  override def requiresAllInputs: Boolean = intercepted.requiresAllInputs

  /**
    * Generates its own guid, different from intercepted
    */
  override val guid: String = UUID.randomUUID().toString

  /**
    * Same values as the intercepted action.
    */
  override val inputLabels: List[String] = intercepted.inputLabels

  /**
    * Same values as the intercepted action.
    */
  override val outputLabels: List[String] = intercepted.outputLabels

  final override def schedulingGuid: String = intercepted.schedulingGuid

  /**
    * Perform the action
    *
    * @param inputs      the [[DataFlowEntities]] corresponding to the [[inputLabels]]
    * @param flowContext context of the flow in which this action runs
    * @return the action outputs (these must be declared in the same order as their labels in [[outputLabels]])
    */
  final override def performAction[C <: FlowContext](inputs: DataFlowEntities, flowContext: C): Try[ActionResult] = {
    val res = instead(inputs, flowContext)
    res
  }

  /**
    * This method calls the intercepted.performAction and if overridden can suppress the default behaviour of the flow
    * and execute something else.
    * It is useful for exploration and debugging work flows, when a user wants to temporarily change the behaviour without
    * touching the main code.
    *
    * By default it calls intercepted.performAction.
    *
    * @param inputs
    * @param flowContext
    * @return
    */
  def instead(inputs: DataFlowEntities, flowContext: FlowContext): Try[ActionResult] = intercepted.performAction(inputs, flowContext)

  /**
    * Calls the intercepted.flowState.
    *
    * @param inputs - action will study the state of the inputs in order to generate self assessment
    * @return - an instance of the DataFlowActionState
    */
  override def flowState(inputs: DataFlowEntities): DataFlowActionState = intercepted.flowState(inputs)

}
