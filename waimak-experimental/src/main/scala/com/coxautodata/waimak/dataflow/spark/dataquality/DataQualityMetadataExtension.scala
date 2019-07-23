package com.coxautodata.waimak.dataflow.spark.dataquality

import com.coxautodata.waimak.dataflow.spark.SparkDataFlow
import com.coxautodata.waimak.dataflow.{DataFlowMetadataExtension, DataFlowMetadataExtensionIdentifier}
import com.coxautodata.waimak.log.Logging
import org.apache.spark.sql.Dataset

case class DataQualityMetadataExtension[CheckType <: DataQualityCheck[CheckType]](meta: Seq[DataQualityMeta[CheckType]])
  extends DataFlowMetadataExtension[SparkDataFlow] with Logging {

  override def identifier: DataFlowMetadataExtensionIdentifier = DataQualityMetadataExtensionIdentifier[CheckType]()

  override def preExecutionManipulation(flow: SparkDataFlow): SparkDataFlow = {
    meta
      .groupBy(m => (m.label, m.alertHandlers))
      .mapValues(_.map(_.check).reduce(_ ++ _))
      .map {
        case ((label, alertHandler), check) => DataQualityMeta(label, alertHandler, check)
      }.foldLeft(flow)((f, m) => {
      f.cacheAsParquet(m.label)
        .doSomething(m.label, m.check.getAlerts(m.label, _).foreach(a => m.alertHandlers.foreach(_.handleAlert(a))))
    })
      .updateMetadataExtension[DataQualityMetadataExtension[CheckType]](identifier, _ => None)
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

  def ++(other: Self): Self

  def getAlerts(label: String, data: Dataset[_]): Seq[DataQualityAlert]
}
