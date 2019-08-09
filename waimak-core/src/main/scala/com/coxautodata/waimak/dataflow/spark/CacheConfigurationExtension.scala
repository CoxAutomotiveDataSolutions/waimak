package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow.DataFlowConfigurationExtension

trait CacheConfigurationExtension extends DataFlowConfigurationExtension[SparkDataFlow] {
  override def preExecutionManipulation(flow: SparkDataFlow): SparkDataFlow = {
    val conf = CaseClassConfigParser[CacheConfigurationExtensionConf](flow.flowContext, s"waimak.dataflow.extensions.$extensionKey.")
    val labelsToCache = if (conf.cacheAll) flow.actions.flatMap(_.outputLabels) else conf.cacheLabels
    cacheLabels(flow, labelsToCache)
  }

  def cacheLabels(flow: SparkDataFlow, labelsToCache: Seq[String]): SparkDataFlow
}

class CacheAsParquetConfigurationExtension extends CacheConfigurationExtension {
  override def extensionKey: String = "cacheasparquet"

  override def cacheLabels(flow: SparkDataFlow, labelsToCache: Seq[String]): SparkDataFlow = flow.cacheAsParquet(labelsToCache: _*)
}

class SparkCacheConfigurationExtension extends CacheConfigurationExtension {

  override def extensionKey: String = "sparkcache"

  override def cacheLabels(flow: SparkDataFlow, labelsToCache: Seq[String]): SparkDataFlow = flow.sparkCache(labelsToCache.head, labelsToCache.tail: _*)
}

private[spark] case class CacheConfigurationExtensionConf(cacheAll: Boolean = false, cacheLabels: List[String] = List.empty)