package com.coxautodata.waimak.spark.app

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import org.apache.spark.sql.SparkSession

/**
  * Performs create and cleanup operations for the [[Env]] implementation used by a provided implementation of [[SparkApp]]
  * The following configuration values should be present in the SparkSession:
  *
  * spark.waimak.environment.appClassName: the application class to use (must extend [[SparkApp]])
  * spark.waimak.environment.actions: the environment action to perform (create or cleanup)
  *
  * The [[Env]] implementation expects configuration values prefixed with spark.waimak.environment.
  */
object EnvironmentManager {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    performEnvironmentAction(spark)
  }

  def performEnvironmentAction(sparkSession: SparkSession): Unit = {
    val environmentAction = CaseClassConfigParser[EnvironmentAction](sparkSession.sparkContext.getConf, "spark.waimak.environment.")
    val app = MultiAppRunner.instantiateApp(environmentAction.appClassName)
    environmentAction.action.toLowerCase() match {
      case "create" => app.createEnv(sparkSession, "spark.waimak.environment.")
      case "cleanup" => app.cleanupEnv(sparkSession, "spark.waimak.environment.")
      case _ => throw new UnsupportedOperationException(s"Unsupported environment action: ${environmentAction.action}")
    }
  }
}

case class EnvironmentAction(action: String, appClassName: String)
