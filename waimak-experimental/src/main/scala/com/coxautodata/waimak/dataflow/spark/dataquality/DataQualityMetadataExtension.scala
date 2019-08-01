package com.coxautodata.waimak.dataflow.spark.dataquality

import com.coxautodata.waimak.dataflow.spark.SparkDataFlow
import com.coxautodata.waimak.dataflow.{DataFlowMetadataExtension, DataFlowMetadataExtensionIdentifier}
import com.coxautodata.waimak.log.Logging
import org.apache.spark.sql.Dataset

import scala.util.{Success, Try}

case class DataQualityMetadataExtension[CheckType <: DataQualityCheck[CheckType]](meta: Seq[DataQualityMeta[CheckType]])
  extends DataFlowMetadataExtension[SparkDataFlow] with Logging {

  override def identifier: DataFlowMetadataExtensionIdentifier = DataQualityMetadataExtensionIdentifier[CheckType]()

  override def preExecutionManipulation(flow: SparkDataFlow): SparkDataFlow = {
    val reducedChecks = meta
      .groupBy(m => (m.label, m.alertHandlers))
      .mapValues(_.map(_.check).reduce(_ ++ _))

    validateChecks(reducedChecks.values.toSeq)

    reducedChecks
      .map {
        case ((label, alertHandler), check) => DataQualityMeta(label, alertHandler, check)
      }
      .groupBy(_.label)
      .foldLeft(flow)((f, m) => {
        val (label, metaForLabel) = m
        f.cacheAsParquet(label)
          .doSomething(label, ds =>
            metaForLabel
              .foreach(
                meta => meta.check.getAlerts(meta.label, ds)
                  .foreach(a => meta.alertHandlers.filter(_.isHandledAlertImportance(a.importance)).foreach(_.handleAlert(a)))
              )
          )
      })
      .updateMetadataExtension[DataQualityMetadataExtension[CheckType]](identifier, _ => None)
  }

  def validateChecks(checks: Seq[CheckType]): Unit = {
    checks.map(_.validateCheck).reduce(_.orElse(_)).get
  }
}


case class DataQualityMetadataExtensionIdentifier[CheckType <: DataQualityCheck[CheckType]]() extends DataFlowMetadataExtensionIdentifier

case class DataQualityMeta[CheckType <: DataQualityCheck[CheckType]](label: String
                                                                     , alertHandlers: Seq[DataQualityAlertHandler]
                                                                     , check: CheckType)

trait DataQualityResult {
  def alerts(label: String): Seq[DataQualityAlert]
}


trait DataQualityCheck[Self <: DataQualityCheck[Self]] {

  def validateCheck: Try[Unit] = Success()

  def ++(other: Self): Self

  def getAlerts(label: String, data: Dataset[_]): Seq[DataQualityAlert]
}
