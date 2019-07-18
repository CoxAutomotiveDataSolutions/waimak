package com.coxautodata.waimak.dataflow.spark.deequ

import com.amazon.deequ.{VerificationResult, VerificationRunBuilder, VerificationSuite}
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow

object DataQualityActions {

  implicit class DataQualityActionImplicits(sparkDataFlow: SparkDataFlow) {

    def addDeequValidation(label: String, checks: VerificationRunBuilder => VerificationRunBuilder): SparkDataFlow = {
      sparkDataFlow
        .cacheAsParquet(label)
        .transform(label)(s"${label}_check")(df => {
          val result = checks(VerificationSuite()
            .onData(df.toDF()))
            .run()
          VerificationResult
            .checkResultsAsDataFrame(sparkDataFlow.flowContext.spark,
              result)
        })
        .show(s"${label}_check")
    }
  }
}
