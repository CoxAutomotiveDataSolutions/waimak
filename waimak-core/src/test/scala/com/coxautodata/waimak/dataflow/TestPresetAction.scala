package com.coxautodata.waimak.dataflow

/**
  * Created by Alexei Perelighin on 11/01/18.
  */
class TestPresetAction(val inputLabels: List[String], val outputLabels: List[String]
                       , output: () => ActionResult
                       , override val requiresAllInputs: Boolean = true) extends DataFlowAction[EmptyFlowContext] {

  override def performAction(inputs: DataFlowEntities, flowContext: EmptyFlowContext): ActionResult = output()

}
