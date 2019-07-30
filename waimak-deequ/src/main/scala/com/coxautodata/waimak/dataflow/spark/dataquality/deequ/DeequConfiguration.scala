package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import com.coxautodata.waimak.dataflow.DataFlowConfigurationExtension
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow
import com.coxautodata.waimak.dataflow.spark.dataquality.DataQualityConfigurationExtension

class DeequConfiguration extends DataQualityConfigurationExtension{
  override def extensionKey: String = "deequ"

  override def preExecutionManipulation(flow: SparkDataFlow): SparkDataFlow = {
    val alerters = getConfiguredAlertHandlers(flow.flowContext)
    ???
  }
}
