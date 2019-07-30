package com.coxautodata.waimak.dataflow.spark.dataquality

import com.coxautodata.waimak.dataflow.DataFlowConfigurationExtension
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow

trait DataQualityConfigurationExtension extends DataFlowConfigurationExtension[SparkDataFlow] {

  def getConfiguredAlertHandlers(flow: SparkDataFlow): List[DataQualityAlertHandler] = ???

}

object DataQualityConfigurationExtension {
  
}
