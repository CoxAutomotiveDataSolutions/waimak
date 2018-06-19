package com.coxautodata.waimak.dataflow

/**
  * Created by Alexei Perelighin on 11/01/18.
  */
class TestInputAction(val inputLabels: List[String], val outputLabels: List[String]
                      , output: DataFlowEntities[String] => ActionResult[String]
                      , override val requiresAllInputs: Boolean = true) extends DataFlowAction[String, EmptyFlowContext] {

  override def performAction(inputs: DataFlowEntities[String], flowContext: EmptyFlowContext): ActionResult[String] = output(inputs)

}
