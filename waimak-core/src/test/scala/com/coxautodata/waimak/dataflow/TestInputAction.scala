package com.coxautodata.waimak.dataflow

import scala.util.Try

/**
  * Created by Alexei Perelighin on 11/01/18.
  */
class TestInputAction(val inputLabels: List[String], val outputLabels: List[String]
                      , output: DataFlowEntities => ActionResult
                      , override val requiresAllInputs: Boolean = true) extends DataFlowAction[EmptyFlowContext] {

  override def performAction(inputs: DataFlowEntities, flowContext: EmptyFlowContext): Try[ActionResult] = Try(output(inputs))

}
