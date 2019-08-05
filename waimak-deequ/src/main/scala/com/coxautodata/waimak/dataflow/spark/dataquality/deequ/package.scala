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

    /**
      * Add Deequ validation for a given label (https://github.com/awslabs/deequ)
      *
      * @param label         the label to perform the validation for
      * @param checks        the Deequ validation to perform e.g.
      *                      `_.addCheck(
      *                      Check(CheckLevel.Error, "unit testing my data")
      *                      .hasSize(_ == 5) // we expect 5 rows
      *                      .isComplete("id") // should never be NULL
      *                      )`
      * @param alertHandler  the alert handler to use for handling alerts for this check
      * @param alertHandlers additional alert handlers to use
      */
    def addDeequValidation(label: String
                           , checks: VerificationRunBuilder => VerificationRunBuilder
                           , alertHandler: DataQualityAlertHandler,
                           alertHandlers: DataQualityAlertHandler*): SparkDataFlow = {
      sparkDataFlow
        .addDataQualityCheck(label, DeequCheck(checks, maybeMetadata = getMeta), alertHandler, alertHandlers: _*)
    }


    /**
      * Adds Deequ validation which uses a metrics repository (see Deequ docs on metrics repositories for more information)
      * N.B in order for this to work, you must set a metrics repository using `setDeequMetricsRepository` or `setDeequStorageLayerMetricsRepository`
      *
      * @param label             the label to perform the validation for
      * @param checksWithMetrics the Deequ validation to perform e.g.
      *                          `_.addAnomalyCheck(RateOfChangeStrategy(maxRateDecrease = Some(0.2)), Completeness("col1")))`
      * @param alertHandler      the alert handler to use for handling alerts for this check
      * @param alertHandlers     additional alert handlers to use
      */
    def addDeequValidationWithMetrics(label: String
                                      , checksWithMetrics: VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository
                                      , alertHandler: DataQualityAlertHandler,
                                      alertHandlers: DataQualityAlertHandler*): SparkDataFlow = {
      sparkDataFlow
        .addDataQualityCheck(label, DeequCheck(metricsRepositoryChecks = Some(checksWithMetrics), maybeMetadata = getMeta), alertHandler, alertHandlers: _*)
    }

    /**
      * Adds Deequ Checks for a label
      *
      * @param label         the label to perform the validation for
      * @param check         first Deequ Check to perform
      * @param checks        additional Deequ checks to perform
      * @param alertHandler  the alert handler to use for handling alerts for this check
      * @param alertHandlers additional alert handlers to use
      */
    def addDeequCheck(label: String
                      , check: Check
                      , checks: Check*)
                     (alertHandler: DataQualityAlertHandler,
                      alertHandlers: DataQualityAlertHandler*): SparkDataFlow = {
      val f = (in: VerificationRunBuilder) => (check +: checks).foldLeft(in)((z, c) => z.addCheck(c))
      sparkDataFlow
        .addDeequValidation(label, f, alertHandler, alertHandlers: _*)
    }

    /**
      * Sets the Deequ metrics repository to use for for Deequ checks using metrics, specified using `addDeequValidationWithMetrics`
      *
      * @param builder        Deequ metrics repository builder
      * @param appendDateTime the date time to be used as a key in the metrics repository (defaults to now)
      */
    def setDeequMetricsRepository(builder: DeequMetricsRepositoryBuilder, appendDateTime: ZonedDateTime = ZonedDateTime.now()): SparkDataFlow = {
      sparkDataFlow
        .updateMetadataExtension[DeequMetadata](DeequDataFlowMetadataExtensionIdentifier, _ => Some(DeequMetadata(builder, appendDateTime)))
    }

    /** Sets the Deequ metrics repository to use for for Deequ checks using metrics, specified using `addDeequValidationWithMetrics`
      * The metrics repository will be a [[StorageLayerMetricsRepository]]
      *
      * @param storageBasePath the base path to use for the [[StorageLayerMetricsRepository]]
      * @param appendDateTime  the date time to be used as a key in the metrics repository (defaults to now)
      */
    def setDeequStorageLayerMetricsRepository(storageBasePath: String, appendDateTime: ZonedDateTime = ZonedDateTime.now()): SparkDataFlow = {
      val builder = (label: String) => new StorageLayerMetricsRepository(new Path(storageBasePath), label, sparkDataFlow.flowContext)
      sparkDataFlow
        .updateMetadataExtension[DeequMetadata](DeequDataFlowMetadataExtensionIdentifier, _ => Some(DeequMetadata(builder, appendDateTime)))
    }

  }

}
