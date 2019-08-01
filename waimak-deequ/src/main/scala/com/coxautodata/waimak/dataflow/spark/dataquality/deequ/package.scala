package com.coxautodata.waimak.dataflow.spark.dataquality

import java.time.ZonedDateTime

import com.amazon.deequ.checks.Check
import com.amazon.deequ.{StorageLayerMetricsRepository, VerificationRunBuilder, VerificationRunBuilderWithRepository}
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow
import com.coxautodata.waimak.dataflow.spark.dataquality.deequ.DeequMetadata.DeequMetricsRepositoryBuilder
import org.apache.hadoop.fs.Path

package object deequ {

  implicit class DeequActionImplicits(sparkDataFlow: SparkDataFlow) {

    private def getMeta: Option[DeequMetadata] = sparkDataFlow.metadataExtensions.collectFirst { case d: DeequMetadata => d }

    def addDeequValidation(label: String
                           , checks: VerificationRunBuilder => VerificationRunBuilder
                           , alertHandler: DataQualityAlertHandler,
                           alertHandlers: DataQualityAlertHandler*): SparkDataFlow = {
      sparkDataFlow
        .addDataQualityCheck(label, DeequCheck(checks, maybeMetadata = getMeta), alertHandler, alertHandlers: _*)
    }

    def addDeequValidationWithMetrics(label: String
                                      , anomalyChecks: VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository
                                      , alertHandler: DataQualityAlertHandler,
                                      alertHandlers: DataQualityAlertHandler*): SparkDataFlow = {
      sparkDataFlow
        .addDataQualityCheck(label, DeequCheck(anomalyChecks = Some(anomalyChecks), maybeMetadata = getMeta), alertHandler, alertHandlers: _*)
    }

    def addDeequCheck(label: String
                      , check: Check
                      , checks: Check*)
                     (alertHandler: DataQualityAlertHandler,
                      alertHandlers: DataQualityAlertHandler*): SparkDataFlow = {
      val f = (in: VerificationRunBuilder) => (check +: checks).foldLeft(in)((z, c) => z.addCheck(c))
      sparkDataFlow
        .addDeequValidation(label, f, alertHandler, alertHandlers: _*)
    }

    def setDeequMetricsRepository(builder: DeequMetricsRepositoryBuilder, appendDateTime: ZonedDateTime = ZonedDateTime.now()): SparkDataFlow = {
      sparkDataFlow
        .updateMetadataExtension[DeequMetadata](DeequDataFlowMetadataExtensionIdentifier, _ => Some(DeequMetadata(builder, appendDateTime)))
    }

    def setDeequStorageLayerMetricsRepository(storageBasePath: String, appendDateTime: ZonedDateTime = ZonedDateTime.now()): SparkDataFlow = {
      val builder = (label: String) => new StorageLayerMetricsRepository(new Path(storageBasePath), label, sparkDataFlow.flowContext)
      sparkDataFlow
        .updateMetadataExtension[DeequMetadata](DeequDataFlowMetadataExtensionIdentifier, _ => Some(DeequMetadata(builder, appendDateTime)))
    }

  }

}
