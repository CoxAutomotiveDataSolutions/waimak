package com.coxautodata.waimak.dataflow.spark.dataquality

import java.util.ServiceLoader

import com.coxautodata.waimak.dataflow.{DataFlowConfigurationExtension, DataFlowException}
import com.coxautodata.waimak.dataflow.spark.{SparkDataFlow, SparkFlowContext}
import com.coxautodata.waimak.dataflow.spark.dataquality.DataQualityConfigurationExtension._

import scala.jdk.CollectionConverters._

trait DataQualityConfigurationExtension extends DataFlowConfigurationExtension[SparkDataFlow] {

  def getConfiguredAlertHandlers(context: SparkFlowContext): List[DataQualityAlertHandler] = {
    val neededServices = context.getStringList(s"spark.$DATAQUALITY_ALERTERS", List.empty)
    val foundServices = ServiceLoader.load(classOf[DataQualityAlertHandlerService]).asScala.toList.filter(e => neededServices.contains(e.handlerKey)).map(e => e.handlerKey -> e).toMap
    val missingServices = neededServices.toSet.diff(foundServices.keySet)
    if (missingServices.nonEmpty) throw new DataFlowException(s"Failed to find the following alert handler services: [${missingServices.mkString(",")}]")
    neededServices
      .map(foundServices)
      .map(_.getAlertHandler(context))
  }

}

object DataQualityConfigurationExtension {
  val DATAQUALITY_ALERTERS: String = "waimak.dataquality.alerters"
}
