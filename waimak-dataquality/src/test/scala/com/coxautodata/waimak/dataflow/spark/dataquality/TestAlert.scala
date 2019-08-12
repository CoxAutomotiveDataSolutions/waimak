package com.coxautodata.waimak.dataflow.spark.dataquality

import java.util.UUID

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import com.coxautodata.waimak.dataflow.spark.dataquality.DataQualityConfigurationExtension.DATAQUALITY_ALERTERS

import scala.util.Try

private object TestAlert {
  private var alerts: Map[String, List[DataQualityAlert]] = Map.empty

  def getAlerts(uuid: UUID): List[DataQualityAlert] = alerts.getOrElse(uuid.toString, List.empty)

  def addAlert(uuid: UUID, alert: DataQualityAlert): Unit = synchronized {
    val existing = getAlerts(uuid)
    alerts = alerts.updated(uuid.toString, alert +: existing)
  }
}

class TestAlert(val testUUID: UUID = UUID.randomUUID(), val alertOn: List[AlertImportance] = List.empty) extends DataQualityAlertHandler {

  def alerts: List[DataQualityAlert] = TestAlert.getAlerts(testUUID)

  override def handleAlert(alert: DataQualityAlert): Try[Unit] = Try {
    TestAlert.addAlert(testUUID, alert)
  }

}

class TestAlertService extends DataQualityAlertHandlerService {
  override def handlerKey: String = "test"

  override def getAlertHandler(flowContext: SparkFlowContext): DataQualityAlertHandler = {
    val conf = CaseClassConfigParser[TestAlertConfig](flowContext, s"${DATAQUALITY_ALERTERS}.test.")
    new TestAlert(conf.testUUID, conf.alertOnImportances)
  }
}

private[dataquality] case class TestAlertConfig(uuid: String, alertOn: List[String]) {
  def alertOnImportances: List[AlertImportance] = alertOn.map(AlertImportance(_))

  def testUUID: UUID = UUID.fromString(uuid)
}