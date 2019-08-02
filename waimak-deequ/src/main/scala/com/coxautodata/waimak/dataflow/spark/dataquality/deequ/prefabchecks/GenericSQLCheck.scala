package com.coxautodata.waimak.dataflow.spark.dataquality.deequ.prefabchecks

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.{VerificationRunBuilder, VerificationRunBuilderWithRepository}
import com.coxautodata.waimak.dataflow.spark.dataquality.deequ.DeequPrefabCheck

class GenericSQLCheck extends DeequPrefabCheck[GenericSQLCheckConfig] {
  override protected def checks(conf: GenericSQLCheckConfig): Option[VerificationRunBuilder => VerificationRunBuilder] = {
    val warningChecks = conf
      .warningChecks
      .foldLeft(Check(CheckLevel.Warning, "warning_checks"))((check, sqlCondition) => check.satisfies(sqlCondition, "generic sql constraint"))

    val criticalChecks = conf
      .criticalChecks
      .foldLeft(Check(CheckLevel.Error, "critical_checks"))(
        (check, sqlCondition) =>
          check.satisfies(sqlCondition, "generic sql constraint")
      )

    Some(_.addChecks(Seq(warningChecks, criticalChecks)))
  }

  override protected def anomalyChecks(conf: GenericSQLCheckConfig): Option[VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository] = None

  override def checkName: String = "genericSQLCheck"
}

case class GenericSQLCheckConfig(warningChecks: Seq[String] = Nil, criticalChecks: Seq[String] = Nil)