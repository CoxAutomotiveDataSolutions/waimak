package com.coxautodata.waimak.dataflow

import scala.util.Try

/**
  * Created by Alexei Perelighin on 11/01/18.
  */
class TestInputAction(val inputLabels: List[String], val outputLabels: List[String]
                      , output: DataFlowEntities => ActionResult
                      , override val requiresAllInputs: Boolean = true) extends DataFlowAction {

  override def performAction[C <: FlowContext](inputs: DataFlowEntities, flowContext: C): Try[ActionResult] = Try(output(inputs))

}
