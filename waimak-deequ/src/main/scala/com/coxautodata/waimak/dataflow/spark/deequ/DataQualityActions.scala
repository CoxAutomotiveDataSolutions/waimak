package com.coxautodata.waimak.dataflow.spark.deequ

import com.amazon.deequ.{VerificationResult, VerificationRunBuilder, VerificationSuite}
import com.coxautodata.waimak.dataflow.spark.{DataQualityAlertHandler, SparkDataFlow}
import com.coxautodata.waimak.dataflow.{DataFlowMetadataExtension, DataFlowMetadataExtensionIdentifier}
import com.coxautodata.waimak.log.Logging
import org.apache.spark.sql.Dataset

case class DataQualityMetadataExtension[CheckType <: DataQualityCheck[CheckType]](meta: Seq[DataQualityMeta[CheckType]])
  extends DataFlowMetadataExtension[SparkDataFlow] with Logging {

  override def identifier: DataFlowMetadataExtensionIdentifier = DataQualityMetadataExtensionIdentifier[CheckType]()

  override def preExecutionManipulation(flow: SparkDataFlow): SparkDataFlow = {
    meta.groupBy(m => (m.label, m.alertHandler))
      .mapValues(_.map(_.check).reduce(_ ++ _))
      .map {
        case ((label, alertHandler), check) => DataQualityMeta(label, alertHandler, check)
      }.foldLeft(flow)((f, m) => {
      f.doSomething(m.label, m.check.getResult(_).alert(m.alertHandler))
    })
  }
}


case class DataQualityMetadataExtensionIdentifier[CheckType <: DataQualityCheck[CheckType]]() extends DataFlowMetadataExtensionIdentifier

case class DataQualityMeta[CheckType <: DataQualityCheck[CheckType]](label: String
                                                                     , alertHandler: DataQualityAlertHandler
                                                                     , check: CheckType)


trait DataQualityResult {
  def alert(alertHandler: DataQualityAlertHandler): Unit
}

class DeequResult extends DataQualityResult {

  override def alert(alertHandler: DataQualityAlertHandler): Unit = ???
}

trait DataQualityCheck[Self <: DataQualityCheck[Self]] {

  def ++(other: Self): Self

  def getResult(data: Dataset[_]): DataQualityResult
}

case class DeequResult(verificationResult: VerificationResult) extends DataQualityResult {
  override def alert(alertHandler: DataQualityAlertHandler): Unit = {

  }
}

case class DeequCheck(checks: VerificationRunBuilder => VerificationRunBuilder) extends DataQualityCheck[DeequCheck] {

  override def ++(other: DeequCheck): DeequCheck = DeequCheck(checks andThen other.checks)

  override def getResult(data: Dataset[_]): DataQualityResult = {
    DeequResult(checks(VerificationSuite()
      .onData(data.toDF))
      .run())
  }
}

object DataQualityActions {

  implicit class DataQualityActionImplicits(sparkDataFlow: SparkDataFlow) {

    def addDeequValidation(label: String
                           , checks: VerificationRunBuilder => VerificationRunBuilder
                           ,): SparkDataFlow = {
      sparkDataFlow
        .cacheAsParquet(label).transform(label)(s"${label}_check")(df => {
        val result = checks(VerificationSuite()
          .onData(df.toDF()))
          .run()
        val resultsForAllConstraints = result.checkResults
          .flatMap { case (_, checkResult) => checkResult.constraintResults }
        resultsForAllConstraints.foreach(println)
        VerificationResult
          .checkResultsAsDataFrame(sparkDataFlow.flowContext.spark,
            result)
      })
        .show(s"${label}_check")
    }
  }

}
