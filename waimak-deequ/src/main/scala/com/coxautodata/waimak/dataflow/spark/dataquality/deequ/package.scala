package com.coxautodata.waimak.dataflow.spark.dataquality

import com.amazon.deequ.VerificationRunBuilder
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow

package object deequ {

  implicit class DeequActionImplicits(sparkDataFlow: SparkDataFlow) {

    def addDeequValidation(label: String
                           , checks: VerificationRunBuilder => VerificationRunBuilder
                           , alertHandler: DataQualityAlertHandler): SparkDataFlow = {
      sparkDataFlow
        .addDataQualityCheck(label, DeequCheck(checks), alertHandler)
    }
  }

}
