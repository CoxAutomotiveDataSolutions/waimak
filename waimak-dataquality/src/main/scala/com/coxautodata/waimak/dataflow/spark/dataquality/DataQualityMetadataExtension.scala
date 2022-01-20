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
          .inPlaceTransform(label)(ds => {
            metaForLabel
              .foreach(
                meta => meta.check.getAlerts(meta.label, ds)
                  .flatMap(a => meta.alertHandlers.filter(_.isHandledAlertImportance(a.importance)).map(_.handleAlert(a)))
                  .foreach(_.get)
              )
            ds
          })
      })
      .updateMetadataExtension[DataQualityMetadataExtension[CheckType]](identifier, _ => None)
  }

  def validateChecks(checks: Seq[CheckType]): Unit = {
    checks.map(_.validateCheck).reduce(_.orElse(_)).get
  }
}


case class DataQualityMetadataExtensionIdentifier[CheckType <: DataQualityCheck[CheckType]]() extends DataFlowMetadataExtensionIdentifier

/**
  * Defines a data quality check to be performed on a label, along with the alert handlers to be used
  *
  * @param label         the label of the dataset on which the check should be performed
  * @param alertHandlers the alert handlers to be used
  * @param check         the data quality check to be performed
  * @tparam CheckType the type of the data quality check
  */
case class DataQualityMeta[CheckType <: DataQualityCheck[CheckType]](label: String
                                                                     , alertHandlers: Seq[DataQualityAlertHandler]
                                                                     , check: CheckType)


/**
  * Defines a data quality check
  */
trait DataQualityCheck[Self <: DataQualityCheck[Self]] {

  /**
    * Validates the check
    *
    * @return true if this is a valid check, false otherwise
    */
  def validateCheck: Try[Unit] = Success(())

  /**
    * Combine this check with another check to take advantage of potential optimisations
    *
    * @param other the check to be combined with this one
    * @return the two checks combined into a new check
    */
  def ++(other: Self): Self

  /**
    * Get any alerts for this check for a given label and Dataset. Normally, no alerts should be returned if the check
    * passes, although alerting on a successful check could also be a valid use case.
    *
    * @param label the label which the check is being performed on
    * @param data  the Dataset on which to perform the check
    * @return the data quality alerts
    */
  def getAlerts(label: String, data: Dataset[_]): Seq[DataQualityAlert]
}
