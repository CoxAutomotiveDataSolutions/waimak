package com.coxautodata.waimak.dataflow.spark.dataquality

import com.coxautodata.waimak.dataflow.DataFlowException
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext

case class DataQualityAlert(alertMessage: String, importance: AlertImportance)

sealed abstract class AlertImportance(val description: String)

object AlertImportance {

  def apply(name: String): AlertImportance = name.toLowerCase match {
    case "critical" => Critical
    case "warning" => Warning
    case "good" => Good
    case "information" => Information
    case _ => throw new DataFlowException(s"Invalid alert importance name: [$name]")
  }

  case object Critical extends AlertImportance("Critical")

  case object Warning extends AlertImportance("Warning")

  case object Good extends AlertImportance("Good")

  case object Information extends AlertImportance("Information")

}

trait DataQualityAlertHandler {

  def alertOn: List[AlertImportance]

  def isHandledAlertImportance(alertImportance: AlertImportance): Boolean = alertOn.isEmpty || alertOn.contains(alertImportance)

  def handleAlert(alert: DataQualityAlert): Unit
}

trait DataQualityAlertHandlerService {

  def handlerKey: String

  def getAlertHandler(flowContext: SparkFlowContext): DataQualityAlertHandler

}
