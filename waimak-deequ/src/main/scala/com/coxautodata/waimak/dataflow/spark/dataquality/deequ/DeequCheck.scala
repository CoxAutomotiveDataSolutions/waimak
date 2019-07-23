package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.{ConstraintResult, ConstraintStatus}
import com.amazon.deequ.{VerificationResult, VerificationRunBuilder, VerificationSuite}
import com.coxautodata.waimak.dataflow.spark.dataquality._
import org.apache.spark.sql.Dataset


case class DeequCheck(checks: VerificationRunBuilder => VerificationRunBuilder) extends DataQualityCheck[DeequCheck] {

  override def ++(other: DeequCheck): DeequCheck = DeequCheck(checks andThen other.checks)

  override def getAlerts(label: String, data: Dataset[_]): Seq[DataQualityAlert] = {
    val verificationResult = getResult(data)
    verificationResult.status match {
      case CheckStatus.Success => Nil
      case _ => verificationResult
        .checkResults.values
        .flatMap(
          result => result.constraintResults
            .filter(_.status != ConstraintStatus.Success)
            .map(constraintResultToAlert(label, _, getAlertImportance(result.status)))
        )
        .toSeq
    }
  }

  def getResult(data: Dataset[_]): VerificationResult = {
    checks(VerificationSuite()
      .onData(data.toDF))
      .run()
  }


  def constraintResultToAlert(label: String, constraintResult: ConstraintResult, alertImportance: AlertImportance): DataQualityAlert = {
    val message =
      s"""${alertImportance.description} alert for label $label
         | ${constraintResult.constraint} : ${constraintResult.message.getOrElse("")}
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

