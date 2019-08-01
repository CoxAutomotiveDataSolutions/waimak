package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import com.amazon.deequ.{VerificationRunBuilder, VerificationRunBuilderWithRepository}
import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext

import scala.reflect.runtime.universe.TypeTag

sealed trait DeequPrefabCheckService {
  def checkName: String

  def getChecks(context: SparkFlowContext, prefix: String): Option[VerificationRunBuilder => VerificationRunBuilder]

  def getAnomalyChecks(context: SparkFlowContext, prefix: String): Option[VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository]
}

abstract class DeequPrefabCheck[C <: Product : TypeTag] extends DeequPrefabCheckService {


  final def getChecks(context: SparkFlowContext, prefix: String): Option[VerificationRunBuilder => VerificationRunBuilder] = {
    checks(getConf(context, prefix))
  }

  final def getAnomalyChecks(context: SparkFlowContext, prefix: String): Option[VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository] = {
    anomalyChecks(getConf(context, prefix))
  }

  final private def getConf(context: SparkFlowContext, prefix: String): C =
    CaseClassConfigParser[C](context, prefix)

  protected def checks(conf: C): Option[VerificationRunBuilder => VerificationRunBuilder]

  protected def anomalyChecks(conf: C): Option[VerificationRunBuilderWithRepository => VerificationRunBuilderWithRepository]

}
