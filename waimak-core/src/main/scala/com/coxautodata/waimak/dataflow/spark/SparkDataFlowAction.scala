package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow._

import scala.util.{Failure, Try}

trait SparkDataFlowAction extends DataFlowAction {

  override def performAction[C <: FlowContext](inputs: DataFlowEntities, flowContext: C): Try[ActionResult] = {
    if (flowContext.getClass.isAssignableFrom(classOf[SparkFlowContext])) performAction(inputs, flowContext.asInstanceOf[SparkFlowContext])
    else Failure(new DataFlowException(s"SparkDataFlowAction can only work with instances of SparkFlowContext and not with ${flowContext.getClass}"))
  }

  def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult]

}

