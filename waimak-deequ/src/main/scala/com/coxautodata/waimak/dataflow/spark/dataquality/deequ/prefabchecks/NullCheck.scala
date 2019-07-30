package com.coxautodata.waimak.dataflow.spark.dataquality.deequ.prefabchecks

import com.amazon.deequ.{VerificationRunBuilder, VerificationRunBuilderWithRepository}
import com.coxautodata.waimak.dataflow.spark.dataquality.deequ.DeequPrefabCheck

class NullCheck extends DeequPrefabCheck[NullCheckConfig]{
  override protected def checks(conf: NullCheckConfig): Option[VerificationRunBuilder => VerificationRunBuilder] = ???

  override protected def anomalyChecks(conf: NullCheckConfig): Option[VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository] = None

  override def checkName: String = "nullCheck"
}

case class NullCheckConfig(columns: List[String], warningThreshold: Option[Double] = None, criticalThreshold: Option[Double] = None)