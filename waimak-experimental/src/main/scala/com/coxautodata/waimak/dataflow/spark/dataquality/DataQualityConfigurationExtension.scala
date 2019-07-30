package com.coxautodata.waimak.dataflow.spark.dataquality

import java.util.ServiceLoader

import com.coxautodata.waimak.dataflow.spark.dataquality.DataQualityConfigurationExtension._
import com.coxautodata.waimak.dataflow.spark.{SparkDataFlow, SparkFlowContext}
import com.coxautodata.waimak.dataflow.{DataFlowConfigurationExtension, DataFlowException}

import scala.collection.JavaConverters._

trait DataQualityConfigurationExtension extends DataFlowConfigurationExtension[SparkDataFlow] {

  def getConfiguredAlertHandlers(context: SparkFlowContext): List[DataQualityAlertHandler] = {
    val neededServices = context.getStringList(DATAQUALITY_ALERTERS, List.empty)
    val foundServices = ServiceLoader.load(classOf[DataQualityAlertHandlerService]).asScala.toList.filter(e => neededServices.contains(e.handlerKey))
    val missingServices = neededServices.toSet.diff(foundServices.map(_.handlerKey).toSet)
    if (missingServices.nonEmpty) throw new DataFlowException(s"Failed to find the following alert handler services: [${missingServices.mkString(",")}]")
    foundServices
      .map(_.getAlertHandler(context))
  }

}

object DataQualityConfigurationExtension {
  val DATAQUALITY_ALERTERS: String = "spark.waimak.dataquality.alerters"
}
