package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.DataFlow.dataFlowParamPrefix
import com.coxautodata.waimak.dataflow.spark.SparkActionHelpers._
import com.coxautodata.waimak.dataflow.{DataFlowMetadataExtension, DataFlowMetadataExtensionIdentifier}
import com.coxautodata.waimak.log.Logging

case class CacheAsParquetMetadataExtension(cacheMeta: CacheMeta) extends DataFlowMetadataExtension[SparkDataFlow] with Logging {

  import CacheAsParquetMetadataExtension._

  override def preExecutionManipulation(flow: SparkDataFlow): SparkDataFlow = {

    val cacheOnlyReused = flow.flowContext.getBoolean(CACHE_ONLY_REUSED_LABELS, CACHE_ONLY_REUSED_LABELS_DEFAULT)

    cacheMeta
      .cached
      .foldLeft(flow) {
        case (z, (label, (partitions, repartition))) =>
          println(s"$label: ${flow.actions.collect { case f if f.inputLabels.contains(label) => f.logLabel }}")
          if (cacheOnlyReused && flow.actions.count(_.inputLabels.contains(label)) < 2) {
            logInfo(s"Cached label [$label] will not be cached even though a cache was requested as it is not used " +
              s"as input for more than one action")
            z
          }
          else {
            val (df, dfw) = applyRepartitionAndPartitionBy(partitions, repartition)
            SparkInterceptors.addPostCacheAsParquet(z, label)(df)(dfw)
          }
      }
      .updateMetadataExtension[CacheAsParquetMetadataExtension](identifier, _ => None)
  }

  override def identifier: DataFlowMetadataExtensionIdentifier = CacheAsParquetMetadataExtensionIdentifier

  def addOrIgnore(label: String, partitions: Option[Either[Seq[String], Int]], repartition: Boolean): CacheAsParquetMetadataExtension = {
    if (cacheMeta.cached.keySet.contains(label)) this
    else CacheAsParquetMetadataExtension(CacheMeta(cacheMeta.cached.updated(label, (partitions, repartition))))
  }
}

object CacheAsParquetMetadataExtension {
  /**
    * Cache only labels that are used more than once as inputs in the flow.
    * Set to false to force all cache calls to actually happen.
    */
  val CACHE_ONLY_REUSED_LABELS: String = s"$dataFlowParamPrefix.cacheOnlyReusedLabels"
  val CACHE_ONLY_REUSED_LABELS_DEFAULT: Boolean = true

  def addCacheAsParquet(sparkFlow: SparkDataFlow, outputLabel: String, partitions: Option[Either[Seq[String], Int]], repartition: Boolean): SparkDataFlow = {
    sparkFlow
      .updateMetadataExtension[CacheAsParquetMetadataExtension](
      CacheAsParquetMetadataExtensionIdentifier,
      e => Some(e.getOrElse(empty).addOrIgnore(outputLabel, partitions, repartition))
    )
  }

  def empty: CacheAsParquetMetadataExtension = CacheAsParquetMetadataExtension(CacheMeta.empty)
}

case object CacheAsParquetMetadataExtensionIdentifier extends DataFlowMetadataExtensionIdentifier

case class CacheMeta(cached: Map[String, (Option[Either[Seq[String], Int]], Boolean)]) {

  def addOrIgnore(label: String, partitions: Option[Either[Seq[String], Int]], repartition: Boolean): CacheMeta = {
    if (cached.keySet.contains(label)) this
    else CacheMeta(cached.updated(label, (partitions, repartition)))
  }

}

object CacheMeta {
  def empty: CacheMeta = CacheMeta(Map.empty)
}