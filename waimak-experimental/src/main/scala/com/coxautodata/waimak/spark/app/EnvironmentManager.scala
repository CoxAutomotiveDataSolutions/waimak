package com.coxautodata.waimak.spark.app

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import org.apache.spark.sql.SparkSession

/**
  * Created by Vicky Avison on 06/03/19.
  */
object EnvironmentManager {

  def main(args: Array[String]): Unit = {
    SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
  }

  def performEnvironmentAction(sparkSession: SparkSession): Unit = {
    val environmentAction = CaseClassConfigParser[EnvironmentAction](sparkSession.sparkContext.getConf, "spark.waimak.environment.")
    val app = MultiAppRunner.instantiateApp(environmentAction.appClass)
    environmentAction.action.toLowerCase() match {
      case "create" => app.createEnv(sparkSession, "spark.waimak.environment.")
      case "cleanup" => app.cleanupEnv(sparkSession, "spark.waimak.environment.")
      case _ => throw new UnsupportedOperationException(s"Unsupported environment action: ${environmentAction.action}")
    }
  }
}

case class EnvironmentAction(action: String, appClass: String)
