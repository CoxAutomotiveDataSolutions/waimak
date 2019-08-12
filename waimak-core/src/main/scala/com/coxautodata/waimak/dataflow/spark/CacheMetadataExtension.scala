package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.DataFlow.dataFlowParamPrefix
import com.coxautodata.waimak.dataflow.spark.SparkActionHelpers._
import com.coxautodata.waimak.dataflow.{DataFlowMetadataExtension, DataFlowMetadataExtensionIdentifier}
import com.coxautodata.waimak.log.Logging
import org.apache.spark.storage.StorageLevel

case class CacheMetadataExtension(cacheMeta: CacheMeta) extends DataFlowMetadataExtension[SparkDataFlow] with Logging {

  import CacheMetadataExtension._

  override def preExecutionManipulation(flow: SparkDataFlow): SparkDataFlow = {

    val cacheOnlyReused = flow.flowContext.getBoolean(CACHE_ONLY_REUSED_LABELS, CACHE_ONLY_REUSED_LABELS_DEFAULT)

    cacheMeta
      .cached
      .foldLeft(flow) {
        case (z, (label, cacheMetaItem)) =>
          println(s"$label: ${flow.actions.collect { case f if f.inputLabels.contains(label) => f.logLabel }}")
          if (cacheOnlyReused && flow.actions.count(_.inputLabels.contains(label)) < 2) {
            logInfo(s"Cached label [$label] will not be cached even though a cache was requested as it is not used " +
              s"as input for more than one action")
            z
          }
          else {
            cacheMetaItem.cache(z, label)
          }
      }
      .updateMetadataExtension[CacheMetadataExtension](identifier, _ => None)
  }

  override def identifier: DataFlowMetadataExtensionIdentifier = CacheAsParquetMetadataExtensionIdentifier

  def addOrIgnore(label: String, cacheMetaItem: CacheMetaItem): CacheMetadataExtension = {
    if (cacheMeta.cached.keySet.contains(label)) this
    else CacheMetadataExtension(CacheMeta(cacheMeta.cached.updated(label, cacheMetaItem)))
  }
}

object CacheMetadataExtension {
  /**
    * Cache only labels that are used more than once as inputs in the flow.
    * Set to false to force all cache calls to actually happen.
    */
  val CACHE_ONLY_REUSED_LABELS: String = s"$dataFlowParamPrefix.cacheOnlyReusedLabels"
  val CACHE_ONLY_REUSED_LABELS_DEFAULT: Boolean = true

  def addCache(sparkFlow: SparkDataFlow, outputLabel: String, cacheMetaItem: CacheMetaItem): SparkDataFlow = {
    sparkFlow
      .updateMetadataExtension[CacheMetadataExtension](
      CacheAsParquetMetadataExtensionIdentifier,
      e => Some(e.getOrElse(empty).addOrIgnore(outputLabel, cacheMetaItem))
    )
  }

  def addCacheAsParquet(sparkFlow: SparkDataFlow, outputLabel: String, partitions: Option[Either[Seq[String], Int]], repartition: Boolean): SparkDataFlow = {
    addCache(sparkFlow, outputLabel, CacheAsParquetMetaItem(partitions, repartition))
  }

  def addSparkCache(sparkFlow: SparkDataFlow, outputLabel: String, partitions: Option[Int], storageLevel: StorageLevel): SparkDataFlow = {
    addCache(sparkFlow, outputLabel, SparkCacheMetaItem(partitions, storageLevel))
  }

  def empty: CacheMetadataExtension = CacheMetadataExtension(CacheMeta.empty)
}

case object CacheAsParquetMetadataExtensionIdentifier extends DataFlowMetadataExtensionIdentifier

case class CacheMeta(cached: Map[String, CacheMetaItem])

trait CacheMetaItem {
  def cache(flow: SparkDataFlow, label: String): SparkDataFlow
}

case class CacheAsParquetMetaItem(partitions: Option[Either[Seq[String], Int]], repartition: Boolean) extends CacheMetaItem {
  override def cache(flow: SparkDataFlow, label: String): SparkDataFlow = {
    val (df, dfw) = applyRepartitionAndPartitionBy(partitions, repartition)
    SparkInterceptors.addPostCacheAsParquet(flow, label)(df)(dfw)
  }
}

case class SparkCacheMetaItem(partitions: Option[Int], storageLevel: StorageLevel) extends CacheMetaItem {
  override def cache(flow: SparkDataFlow, label: String): SparkDataFlow = {
    SparkInterceptors.addSparkCache(flow, label, storageLevel, applyFileReduce(partitions))
  }
}

object CacheMeta {
  def empty: CacheMeta = CacheMeta(Map.empty)
}