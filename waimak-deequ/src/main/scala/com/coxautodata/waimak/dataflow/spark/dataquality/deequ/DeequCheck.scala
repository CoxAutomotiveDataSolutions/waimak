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

/**
  * An implementation of [[DataQualityCheck]] which uses the Deequ library for performing data validation checks
  * (https://github.com/awslabs/deequ)
  *
  * @param checks                  simple Deequ validation checks
  * @param metricsRepositoryChecks Deequ validation checks which use a metrics repository e.g. anomaly checks
  * @param maybeMetadata           optional Deequ metadata containing information on the metrics repository to use (required
  *                                if there are any metricsRepositoryChecks)
  */
case class DeequCheck(checks: VerificationRunBuilder => VerificationRunBuilder = identity,
                      metricsRepositoryChecks: Option[VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository] = None,
                      maybeMetadata: Option[DeequMetadata]) extends DataQualityCheck[DeequCheck] {

  override def validateCheck: Try[Unit] = {
    (metricsRepositoryChecks, maybeMetadata) match {
      case (Some(_), None) => Failure(
        DeequCheckException("Anomaly checks were specified but no metrics repository was set, or metrics repository was set after anomaly checks were defined. " +
          "Use setDeequMetricsRepository or setDeequStorageLayerMetricsRepository to set a repository and ensure you set the " +
          "repository before calling any checks that need it."))
      case _ => Success()
    }
  }

  override def ++(other: DeequCheck): DeequCheck =
    DeequCheck(
      checks andThen other.checks,
      (metricsRepositoryChecks, other.metricsRepositoryChecks) match {
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
    if (metricsRepositoryChecks.isDefined && maybeMetadata.isEmpty) throw new DataFlowException(s"Error checking metrics for [$label]: A metrics repository must be defined when using anomaly metrics")

    val withChecks = checks(VerificationSuite()
      .onData(data.toDF))

    maybeMetadata
      .map {
        m =>
          val withRepository = withChecks.useRepository(m.repoBuilder(label)).saveOrAppendResult(ResultKey(m.metricsDateTime.toEpochSecond))
          metricsRepositoryChecks.map(_.apply(withRepository))
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