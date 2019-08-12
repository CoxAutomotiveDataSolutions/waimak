package com.coxautodata.waimak.dataflow.spark.dataquality

import org.apache.spark.sql.Dataset

/**
  * A simple implementation of DataQualityCheck which doesn't perform any optimisation on check concatenation.
  * Only use this if it's really necessary, otherwise use the Deequ implementation in the waimak-deequ module.
  */
case class DatasetChecks(checks: Seq[DatasetCheck[_]]) extends DataQualityCheck[DatasetChecks] {

  override def ++(other: DatasetChecks): DatasetChecks = DatasetChecks(checks ++ other.checks)

  override def getAlerts(label: String, data: Dataset[_]): Seq[DataQualityAlert] =
    checks.flatMap(_.getAlerts(label, data))
}

trait DatasetCheck[T] {
  def getAlerts(label: String, data: Dataset[_]): Seq[DataQualityAlert]
}


class SimpleDatasetCheck[T](metric: Dataset[_] => Dataset[T]
                            , alert: (Dataset[T], String) => Seq[DataQualityAlert]) extends DatasetCheck[T] {

  def getAlerts(label: String, data: Dataset[_]): Seq[DataQualityAlert] = {
    alert(metric(data), label)
  }

}
