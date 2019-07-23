package com.coxautodata.waimak.dataflow.spark.dataquality

case class DataQualityAlert(alertMessage: String, importance: AlertImportance)

sealed abstract class AlertImportance(val description: String)

case object Critical extends AlertImportance("Critical")

case object Warning extends AlertImportance("Warning")

case object Good extends AlertImportance("Good")

case object Information extends AlertImportance("Information")

trait DataQualityAlertHandler {
  def handleAlert(alert: DataQualityAlert): Unit
}
