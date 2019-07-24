package com.coxautodata.waimak.dataflow.spark.dataquality

import com.amazon.deequ.checks.Check
import com.amazon.deequ.{VerificationRunBuilder, VerificationRunBuilderWithRepository}
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow

package object deequ {

  implicit class DeequActionImplicits(sparkDataFlow: SparkDataFlow) {

    def addDeequValidation(label: String
                           , checks: VerificationRunBuilder => VerificationRunBuilder
                           , alertHandler: DataQualityAlertHandler,
                           alertHandlers: DataQualityAlertHandler*): SparkDataFlow = {
      sparkDataFlow
        .addDataQualityCheck(label, DeequCheck(checks), alertHandler, alertHandlers: _*)
    }

    def addDeequValidationWithMetrics(label: String
                                      , anomalyChecks: VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository
                                      , alertHandler: DataQualityAlertHandler,
                                      alertHandlers: DataQualityAlertHandler*): SparkDataFlow = {
      sparkDataFlow
        .addDataQualityCheck(label, DeequCheck(anomalyChecks = Some(anomalyChecks)), alertHandler, alertHandlers: _*)
    }

    def addDeequCheck(label: String
                      , check: Check
                      , checks: Check*)
                     (alertHandler: DataQualityAlertHandler,
                      alertHandlers: DataQualityAlertHandler*): SparkDataFlow = {
      val f = DeequCheck(v => (check +: checks).foldLeft(v)((z, c) => z.addCheck(c)))
      sparkDataFlow
        .addDataQualityCheck(label, f, alertHandler, alertHandlers: _*)
    }

    //    def addDeequAnomalyCheck(label: String
    //                      , check: Check
    //                      , checks: Check*)
    //                     (alertHandler: DataQualityAlertHandler,
    //                      alertHandlers: DataQualityAlertHandler*): SparkDataFlow = {
    //      val f = DeequCheck(v => (check +: checks).foldLeft(v)((z, c) => z.addCheck(c)))
    //      sparkDataFlow
    //        .addDataQualityCheck(label, f, alertHandler, alertHandlers: _*)
    //    }

  }

}
