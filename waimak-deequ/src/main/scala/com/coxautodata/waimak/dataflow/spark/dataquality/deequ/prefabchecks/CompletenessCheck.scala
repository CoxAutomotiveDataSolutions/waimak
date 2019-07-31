package com.coxautodata.waimak.dataflow.spark.dataquality.deequ.prefabchecks

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.{VerificationRunBuilder, VerificationRunBuilderWithRepository}
import com.coxautodata.waimak.dataflow.spark.dataquality.deequ.DeequPrefabCheck

class CompletenessCheck extends DeequPrefabCheck[CompletenessCheckConfig] {
  override protected def checks(conf: CompletenessCheckConfig): Option[VerificationRunBuilder => VerificationRunBuilder] = {
    val warningChecks = generateChecks(conf.warningThreshold, CheckLevel.Warning, conf.columns, "warning_checks")
    val criticalChecks = generateChecks(conf.criticalThreshold, CheckLevel.Error, conf.columns, "critical_checks")
    Some((b: VerificationRunBuilder) => b.addChecks(warningChecks ++ criticalChecks))
  }

  private def generateChecks(maybeThreshold: Option[Double], level: CheckLevel.Value, columns: List[String], description: String): Seq[Check] = {
    maybeThreshold.toSeq.flatMap {
      threshold =>
        columns.map {
          col =>
            Check(level, description)
              .hasCompleteness(col, _ >= threshold, Some(s"Less than ${threshold * 100}% of $col values were complete."))
        }
    }
  }

  override protected def anomalyChecks(conf: CompletenessCheckConfig): Option[VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository] = None

  override def checkName: String = "completenessCheck"
}

case class CompletenessCheckConfig(columns: List[String], warningThreshold: Option[Double] = None, criticalThreshold: Option[Double] = None)