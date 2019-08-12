package com.coxautodata.waimak.dataflow.spark.dataquality

import com.coxautodata.waimak.dataflow.DataFlowException
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext

import scala.util.Try

/**
  * Knows how to handle data quality alerts
  */
trait DataQualityAlertHandler {

  /**
    * The alert importance levels for which this handler should alert.
    * If this is empty, the handler will alert for all importance levels.
    *
    * @return the list of alert importance levels
    */
  def alertOn: List[AlertImportance]

  /**
    * Whether this handler should alert for the given alert importance level
    *
    * @param alertImportance the alert importance level
    * @return true if this handler should alert, false otherwise
    */
  def isHandledAlertImportance(alertImportance: AlertImportance): Boolean = alertOn.isEmpty || alertOn.contains(alertImportance)

  /**
    * Handle the given data quality alert
    *
    * @param alert the data quality alert
    * @return Success() if the alert was successfully handled, Failure otherwise. If the handler intends for an exception
    *         to be thrown, a Failure should be returned containing the exception. This will be thrown once all non-exception
    *         alerts have been handled.
    */
  def handleAlert(alert: DataQualityAlert): Try[Unit]
}

/**
  * A service for registering data quality alert handlers
  */
trait DataQualityAlertHandlerService {

  /**
    * The key for identifying the alert handler
    */
  def handlerKey: String

  /**
    * Get the alert handler using the given flow context
    */
  def getAlertHandler(flowContext: SparkFlowContext): DataQualityAlertHandler

}


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

