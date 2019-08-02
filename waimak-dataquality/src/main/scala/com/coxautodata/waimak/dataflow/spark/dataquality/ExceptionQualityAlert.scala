package com.coxautodata.waimak.dataflow.spark.dataquality

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import com.coxautodata.waimak.dataflow.spark.dataquality.DataQualityConfigurationExtension.DATAQUALITY_ALERTERS

import scala.util.{Failure, Try}

case class ExceptionQualityAlert(alertOn: List[AlertImportance] = List.empty) extends DataQualityAlertHandler {
  override def handleAlert(alert: DataQualityAlert): Try[Unit] = {
    Failure(new DataQualityAlertException(s"${alert.importance.description}: ${alert.alertMessage}"))
  }
}

class ExceptionQualityAlertService extends DataQualityAlertHandlerService {
  override def handlerKey: String = "exception"

  override def getAlertHandler(flowContext: SparkFlowContext): DataQualityAlertHandler = {
    val conf = CaseClassConfigParser[ExceptionAlertConfig](flowContext, s"${DATAQUALITY_ALERTERS}.exception.")
    ExceptionQualityAlert(conf.alertOnImportances)
  }
}

private[dataquality] case class ExceptionAlertConfig(alertOn: List[String]) {
  def alertOnImportances: List[AlertImportance] = alertOn.map(AlertImportance(_))
}