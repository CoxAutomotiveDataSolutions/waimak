package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.{ConstraintResult, ConstraintStatus}
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.{VerificationResult, VerificationRunBuilder, VerificationRunBuilderWithRepository, VerificationSuite}
import com.coxautodata.waimak.dataflow.DataFlowException
import com.coxautodata.waimak.dataflow.spark.dataquality.AlertImportance.{Critical, Good, Warning}
import com.coxautodata.waimak.dataflow.spark.dataquality._
import org.apache.spark.sql.Dataset

import scala.util.{Failure, Success, Try}


case class DeequCheck(checks: VerificationRunBuilder => VerificationRunBuilder = identity,
                      anomalyChecks: Option[VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository] = None,
                      maybeMetadata: Option[DeequMetadata]) extends DataQualityCheck[DeequCheck] {

  override def validateCheck: Try[Unit] = {
    (anomalyChecks, maybeMetadata) match {
      case (Some(_), None) => Failure(
        DeequCheckException("Anomaly checks were specified but no metrics repository was set. " +
          "Use setDeequMetricsRepository or setDeequStorageLayerMetricsRepository"))
      case _ => Success()
    }
  }

  override def ++(other: DeequCheck): DeequCheck =
    DeequCheck(
      checks andThen other.checks,
      (anomalyChecks, other.anomalyChecks) match {
        case (Some(a), Some(b)) => Some(a andThen b)
        case (a, b) => a.orElse(b)
      },
      maybeMetadata.orElse(other.maybeMetadata)
    )

  override def getAlerts(label: String, data: Dataset[_]): Seq[DataQualityAlert] = {
    val verificationResult = getResult(label, data)
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

  def getResult(label: String, data: Dataset[_]): VerificationResult = {
    if (anomalyChecks.isDefined && maybeMetadata.isEmpty) throw new DataFlowException(s"Error checking metrics for [$label]: A metrics repository must be defined when using anomaly metrics")

    val withChecks = checks(VerificationSuite()
      .onData(data.toDF))

    maybeMetadata
      .map {
        m =>
          val withRepository = withChecks.useRepository(m.repoBuilder(label)).saveOrAppendResult(ResultKey(m.metricsDateTime.toEpochSecond))
          anomalyChecks.map(_.apply(withRepository))
            .getOrElse(withRepository)
      }
      .getOrElse(withChecks)
      .run()
  }


  def constraintResultToAlert(label: String, constraintResult: ConstraintResult, alertImportance: AlertImportance): DataQualityAlert = {
    val message =
      s"""${alertImportance.description} alert for label $label
         | ${constraintResult.constraint} : ${constraintResult.message.getOrElse("")}""".stripMargin
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

case class DeequCheckException(message: String) extends RuntimeException(message)