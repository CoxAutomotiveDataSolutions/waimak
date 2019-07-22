package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import com.amazon.deequ.VerificationResult
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.{ConstraintResult, ConstraintStatus}
import com.coxautodata.waimak.dataflow.spark.dataquality
import com.coxautodata.waimak.dataflow.spark.dataquality.{AlertImportance, Critical, DataQualityAlert, DataQualityResult, Good, Warning}


case class DeequResult(verificationResult: VerificationResult) extends DataQualityResult {
  override def alerts(label: String): Seq[DataQualityAlert] = {
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

  def constraintResultToAlert(label: String, constraintResult: ConstraintResult, alertImportance: AlertImportance): DataQualityAlert = {
    val message =
      s"""${alertImportance.description} alert for label $label
         | ${constraintResult.constraint} : ${constraintResult.message.getOrElse("")}
       """.stripMargin
    dataquality.DataQualityAlert(message, alertImportance)
  }

  def getAlertImportance(checkStatus: CheckStatus.Value): AlertImportance = {
    checkStatus match {
      case CheckStatus.Success => Good
      case CheckStatus.Warning => Warning
      case CheckStatus.Error => Critical
    }
  }
}