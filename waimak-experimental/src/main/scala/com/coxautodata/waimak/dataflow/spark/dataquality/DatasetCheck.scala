package com.coxautodata.waimak.dataflow.spark.dataquality

import org.apache.spark.sql.Dataset

trait DatasetCheck[T] {
  def getAlerts(label: String, data: Dataset[_]): Seq[DataQualityAlert]
}

class SimpleDatasetCheck[T](metric: Dataset[_] => Dataset[T]
                                 , alert: (Dataset[T], String) => Seq[DataQualityAlert]) extends DatasetCheck[T] {

  def getAlerts(label: String, data: Dataset[_]): Seq[DataQualityAlert] = {
    alert(metric(data), label)
  }

}


case class DatasetChecks(checks: Seq[DatasetCheck[_]]) extends DataQualityCheck[DatasetChecks] {

  override def ++(other: DatasetChecks): DatasetChecks = DatasetChecks(checks ++ other.checks)

  override def getAlerts(label: String, data: Dataset[_]): Seq[DataQualityAlert] =
    checks.flatMap(_.getAlerts(label, data))
}
