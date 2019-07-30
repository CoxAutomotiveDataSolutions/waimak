package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow.DataFlowConfigurationExtension

class CacheAsParquetConfigurationExtension extends DataFlowConfigurationExtension[SparkDataFlow] {
  override def extensionKey: String = "cacheasparquet"

  override def preExecutionManipulation(flow: SparkDataFlow): SparkDataFlow = {
    val conf = CaseClassConfigParser[CacheAsParquetConfigurationExtensionConf](flow.flowContext, s"waimak.dataflow.extensions.${extensionKey}.")
    val labelsToCache = if(conf.cacheAll) flow.actions.flatMap(_.outputLabels) else conf.cacheLabels
    flow.cacheAsParquet(labelsToCache:_*)
  }
}

private[spark] case class CacheAsParquetConfigurationExtensionConf(cacheAll: Boolean = false, cacheLabels: List[String] = List.empty)