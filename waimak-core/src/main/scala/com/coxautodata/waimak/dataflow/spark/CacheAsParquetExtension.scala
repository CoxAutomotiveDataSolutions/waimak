package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.DataFlow.dataFlowParamPrefix
import com.coxautodata.waimak.dataflow.{DataFlow, DataFlowExtension, DataFlowMetadataState}

case object CacheAsParquetExtension extends DataFlowExtension {

  /**
    * Cache only labels that are used more than once as inputs in the flow.
    * Set to false to force all cache calls to actually happen.
    */
  val CACHE_ONLY_REUSED_LABELS: String = s"$dataFlowParamPrefix.cacheOnlyReusedLabels"
  val CACHE_ONLY_REUSED_LABELS_DEFAULT: Boolean = true

  override def initialState: DataFlowMetadataState = CacheMeta(Map.empty)

  def addCacheAsParquet(sparkFlow: SparkDataFlow, outputLabel: String, partitions: Option[Either[Seq[String], Int]], repartition: Boolean): SparkDataFlow = ???

  override def preExecutionManipulation[S <: DataFlow[S]](flow: S, meta: DataFlowMetadataState): Option[S] = {

    val cacheMeta = meta.getMetadataAsType[CacheMeta]
    if (cacheMeta.cached.isEmpty) None
    else {
      val cacheOnlyReused = flow.flowContext.getBoolean(CACHE_ONLY_REUSED_LABELS, CACHE_ONLY_REUSED_LABELS_DEFAULT)

      cacheMeta
        .cached
        .foldLeft(flow){
          case (z, (label, (partitions, repartition))) =>
            if (cacheOnlyReused && flow.actions.count(_.inputLabels.contains(label)) < 2) {
              logInfo(s"Committed label [${commitEntry.label}] will not be cached even though cacheLabels hint was given as it is not used " +
                s"as input for any other actions")
              z
            }

        }
    }
  }

}


/*
def addCacheAsParquet(sparkFlow: SparkDataFlow, outputLabel: String, partitions: Option[Either[Seq[String], Int]], repartition: Boolean): SparkDataFlow = {
  val (df, dfw) = applyRepartitionAndPartitionBy(partitions, repartition)
  addPostCacheAsParquet(sparkFlow, outputLabel)(df)(dfw)
}


  //TODO this can be merged with other cache
  private def optionallyCacheLabel(flow: SparkDataFlow, commitEntry: CommitEntry): SparkDataFlow = {
    if (flow.actions.exists(_.inputLabels.contains(commitEntry.label)))
      SparkInterceptors.addCacheAsParquet(flow, commitEntry.label, commitEntry.partitions, commitEntry.repartition)
    else {
      logInfo(s"Committed label [${commitEntry.label}] will not be cached even though cacheLabels hint was given as it is not used " +
        s"as input for any other actions")
      flow
    }
  }
 */

case class CacheMeta(cached: Map[String, (Option[Either[Seq[String], Int]], Boolean)]) extends DataFlowMetadataState