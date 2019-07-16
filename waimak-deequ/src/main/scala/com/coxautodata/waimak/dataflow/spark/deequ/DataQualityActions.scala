package com.coxautodata.waimak.dataflow.spark.deequ

import com.amazon.deequ.checks.Check
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow

object DataQualityActions {

  implicit class DataQualityActionImplicits(sparkDataFlow: SparkDataFlow) {

    def addChecks(label: String, checks: Check*): SparkDataFlow = {
      sparkDataFlow
        .cacheAsParquet(label)
        .transform(label)(s"${label}_check")(df => {
          val result =  VerificationSuite()
            .onData(df.toDF())
            .addChecks(checks)
            .run()
          VerificationResult
            .checkResultsAsDataFrame(sparkDataFlow.flowContext.spark,
              result)
        })
        .show(s"${label}_check")
    }
  }

}
