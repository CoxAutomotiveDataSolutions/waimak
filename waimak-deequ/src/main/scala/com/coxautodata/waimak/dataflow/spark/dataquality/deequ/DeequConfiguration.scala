package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import java.util.ServiceLoader

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow.DataFlowException
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow
import com.coxautodata.waimak.dataflow.spark.dataquality.deequ.DeequConfiguration._
import com.coxautodata.waimak.dataflow.spark.dataquality.{DataQualityAlertHandler, DataQualityConfigurationExtension}

import scala.collection.JavaConverters._

class DeequConfigurationExtension extends DataQualityConfigurationExtension {
  override def extensionKey: String = "deequ"

  override def preExecutionManipulation(flow: SparkDataFlow): SparkDataFlow = {
    val alerters = getConfiguredAlertHandlers(flow.flowContext)
    val conf = CaseClassConfigParser[DeequConfig](flow.flowContext, DEEQU_CONFIG)
    val availableChecks = ServiceLoader.load(classOf[DeequPrefabCheckService]).asScala.toList
    flow
      .foldLeftOver(conf.metricsStoragePath) {
        (z, storagePath) => z.setDeequStorageLayerMetricsRepository(storagePath)
      }
      .foldLeftOver(conf.labelsToMonitor) {
        (z, label) => checksForLabel(z, availableChecks, label, alerters)
      }
  }

  def checksForLabel(flow: SparkDataFlow, availableChecks: Seq[DeequPrefabCheckService], label: String, alerters: List[DataQualityAlertHandler]): SparkDataFlow = {
    val (alertHead, alertTail) = alerters match {
      case h :: t => (h, t)
      case _ => throw new DataFlowException("At least one alerter must be specified when using Deequ")
    }
    val labelBasePrefix = s"${DEEQU_CONFIG}labels.${label}"
    val activeChecks: Seq[String] = flow.flowContext.getStringList(s"spark.$labelBasePrefix.checks", List.empty)
    val foundChecks = availableChecks.filter(a => activeChecks.contains(a.checkName))
    val missingChecks = activeChecks.toSet.diff(availableChecks.map(_.checkName).toSet)
    if (missingChecks.nonEmpty) throw new DataFlowException(s"The following checks for label [$label] could not be found: [${missingChecks.mkString(",")}]")
    flow
      .foldLeftOver(foundChecks) {
        (z, c) =>
          val prefix = s"$labelBasePrefix.${c.checkName}."
          z
            .foldLeftOver(c.getChecks(z.flowContext, prefix)) {
              (zz, checks) =>
                zz.addDeequValidation(label, checks, alertHead, alertTail: _*)
            }
            .foldLeftOver(c.getAnomalyChecks(z.flowContext, prefix)) {
              (zz, anomalyChecks) =>
                zz.addDeequValidationWithMetrics(label, anomalyChecks, alertHead, alertTail: _*)
            }
      }
  }
}

object DeequConfiguration {
  val DEEQU_CONFIG: String = "waimak.dataquality.deequ."

}

private[deequ] case class DeequConfig(labelsToMonitor: List[String], metricsStoragePath: Option[String] = None)

private[deequ] case class DeequLabelChecks(checks: List[String])
