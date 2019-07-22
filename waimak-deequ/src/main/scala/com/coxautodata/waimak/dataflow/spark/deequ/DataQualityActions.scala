package com.coxautodata.waimak.dataflow.spark.deequ

import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.ConstraintResult
import com.amazon.deequ.{VerificationResult, VerificationRunBuilder, VerificationSuite}
import com.coxautodata.waimak.dataflow.spark.{AlertImportance, Critical, DataQualityAlert, DataQualityAlertHandler, Good, SparkDataFlow, Warning}
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
      f.doSomething(m.label, m.check.getResult(_).alerts(m.label).foreach(m.alertHandler.handleAlert))
    })
      .updateMetadataExtension[DataQualityMetadataExtension[CheckType]](identifier, _ => None)
  }
}


case class DataQualityMetadataExtensionIdentifier[CheckType <: DataQualityCheck[CheckType]]() extends DataFlowMetadataExtensionIdentifier

case class DataQualityMeta[CheckType <: DataQualityCheck[CheckType]](label: String
                                                                     , alertHandler: DataQualityAlertHandler
                                                                     , check: CheckType)

trait DataQualityResult {
  def alerts(label: String): Seq[DataQualityAlert]
}


trait DataQualityCheck[Self <: DataQualityCheck[Self]] {

  def ++(other: Self): Self

  def getResult(data: Dataset[_]): DataQualityResult
}

case class DeequResult(verificationResult: VerificationResult) extends DataQualityResult {
  override def alerts(label: String): Seq[DataQualityAlert] = {
    verificationResult.status match {
      case CheckStatus.Success => Nil
      case _ => verificationResult
        .checkResults.values
        .filter(_.status != CheckStatus.Success)
        .flatMap(
          result => result.constraintResults
            .map(constraintResultToAlert(label, _, getAlertImportance(result.status)))
        )
        .toSeq
    }
  }

  def constraintResultToAlert(label: String, constraintResult: ConstraintResult, alertImportance: AlertImportance): DataQualityAlert = {
    val message =
      s"""${alertImportance.description} alert on label $label for constraint ${constraintResult.constraint}
         | ${constraintResult.message.get}
       """.stripMargin
    DataQualityAlert(message, alertImportance)
  }

  def getAlertImportance(checkStatus: CheckStatus.Value): AlertImportance = {
    checkStatus match {
      case CheckStatus.Success => Good
      case CheckStatus.Warning => Warning
      case CheckStatus.Error => Critical
    }
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

    def addDataQualityCheck[CheckType <: DataQualityCheck[CheckType]](label: String
                                                                      , check: CheckType
                                                                      , alertHandler: DataQualityAlertHandler): SparkDataFlow = {
      sparkDataFlow
        .updateMetadataExtension[DataQualityMetadataExtension[CheckType]](DataQualityMetadataExtensionIdentifier[CheckType]()
        , {
          m =>
            val existing = m.map(_.meta).getOrElse(Nil)
            val newMeta = DataQualityMeta(label, alertHandler, check)
            Some(DataQualityMetadataExtension(existing :+ newMeta))
        })
    }

    def addDeequValidation(label: String
                           , checks: VerificationRunBuilder => VerificationRunBuilder
                           , alertHandler: DataQualityAlertHandler): SparkDataFlow = {
      sparkDataFlow
        .addDataQualityCheck(label, DeequCheck(checks), alertHandler)
    }
  }

}
