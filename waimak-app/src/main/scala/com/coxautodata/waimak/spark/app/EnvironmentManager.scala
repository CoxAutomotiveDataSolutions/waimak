package com.coxautodata.waimak.spark.app

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import org.apache.spark.sql.SparkSession

/**
  * Performs create and cleanup operations for the [[Env]] implementation used by a provided implementation of [[SparkApp]]
  * The following configuration values should be present in the SparkSession:
  *
  * spark.waimak.environment.ids: comma-separated unique ids for the environments
  * spark.waimak.environment.{environmentid}.appClassName: the application class to use (must extend [[SparkApp]])
  * spark.waimak.environment.{environmentid}.action: the environment action to perform (create or cleanup)
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
    val environmentActionConf = CaseClassConfigParser[EnvironmentAction](SparkFlowContext(sparkSession), "waimak.environment.")
    environmentActionConf.ids.foreach(performEnvironmentActionForID(sparkSession, _, environmentActionConf.action))
  }

  def performEnvironmentActionForID(sparkSession: SparkSession, id: String, environmentAction: String): Unit = {
    val environmentAppClass =
      CaseClassConfigParser[SingleAppConfig](SparkFlowContext(sparkSession), s"waimak.environment.$id.").appClassName
    val app = MultiAppRunner.instantiateApp(environmentAppClass)
    environmentAction.toLowerCase() match {
      case "create" => app.createEnv(sparkSession, s"waimak.environment.$id.")
      case "cleanup" => app.cleanupEnv(sparkSession, s"waimak.environment.$id.")
      case _ => throw new UnsupportedOperationException(s"Unsupported environment action: $environmentAction")
    }
  }
}

case class EnvironmentAction(ids: Seq[String], action: String)
