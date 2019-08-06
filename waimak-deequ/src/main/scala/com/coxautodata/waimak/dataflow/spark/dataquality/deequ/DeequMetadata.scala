package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import java.time.ZonedDateTime

import com.amazon.deequ.repository.MetricsRepository
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow
import com.coxautodata.waimak.dataflow.spark.dataquality.deequ.DeequMetadata.DeequMetricsRepositoryBuilder
import com.coxautodata.waimak.dataflow.{DataFlowMetadataExtension, DataFlowMetadataExtensionIdentifier}

case class DeequMetadata(repoBuilder: DeequMetricsRepositoryBuilder, metricsDateTime: ZonedDateTime) extends DataFlowMetadataExtension[SparkDataFlow] {
  override def identifier: DataFlowMetadataExtensionIdentifier = DeequDataFlowMetadataExtensionIdentifier

  override def preExecutionManipulation(flow: SparkDataFlow): SparkDataFlow = flow.updateMetadataExtension[DeequMetadata](DeequDataFlowMetadataExtensionIdentifier, _ => None)
}

object DeequMetadata {
  /**
    * A type alias used when setting a metrics repository for Deequ through Waimak actions.
    * Type goes from label to metrics repository.
    */
  type DeequMetricsRepositoryBuilder = String => MetricsRepository
}

case object DeequDataFlowMetadataExtensionIdentifier extends DataFlowMetadataExtensionIdentifier