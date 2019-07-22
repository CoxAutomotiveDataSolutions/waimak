package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import com.amazon.deequ.{VerificationRunBuilder, VerificationSuite}
import com.coxautodata.waimak.dataflow.spark.dataquality._
import org.apache.spark.sql.Dataset


case class DeequCheck(checks: VerificationRunBuilder => VerificationRunBuilder) extends DataQualityCheck[DeequCheck] {

  override def ++(other: DeequCheck): DeequCheck = DeequCheck(checks andThen other.checks)

  override def getResult(data: Dataset[_]): DataQualityResult = {
    DeequResult(checks(VerificationSuite()
      .onData(data.toDF))
      .run())
  }
}

