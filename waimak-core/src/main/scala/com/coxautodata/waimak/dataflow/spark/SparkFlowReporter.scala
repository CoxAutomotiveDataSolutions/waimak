package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.{DataFlowAction, FlowReporter}
import org.apache.spark.sql.Dataset

object SparkFlowReporter extends FlowReporter[SparkFlowContext] {
  override def reportActionStarted(action: DataFlowAction[SparkFlowContext], flowContext: SparkFlowContext): Unit = {
    flowContext.spark.sparkContext.setJobGroup(action.guid, action.description)
  }

  override def reportActionFinished(action: DataFlowAction[SparkFlowContext], flowContext: SparkFlowContext): Unit = {
    flowContext.spark.sparkContext.clearJobGroup()
  }
}