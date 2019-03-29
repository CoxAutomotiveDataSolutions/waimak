package com.coxautodata.waimak.spark.app

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.dataflow.spark.{SimpleAction, SparkDataFlow, SparkFlowContext}
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.{universe => ru}

/**
  * Allows multiple Spark applications to be run in a single main method whilst obeying configured dependency constraints.
  * The following configuration values should be present in the SparkSession:
  *
  * spark.waimak.apprunner.apps: a comma-delimited list of the names (identifiers) of all of the applications being run
  * (e.g. myapp1,myapp2)
  *
  * spark.waimak.apprunner.{appname}.appClassName: for each application, the application class to use (must extend [[SparkApp]])
  * (e.g. spark.waimak.apprunner.myapp1.appClassName = com.example.MyWaimakApp)
  *
  * spark.waimak.apprunner.{appname}.dependencies: for each application, an optional comma-delimited list of dependencies.
  * If omitted, the application will have no dependencies and will not wait for other apps to finish before starting execution.
  * Dependencies must match the names provided in spark.waimak.apprunner.apps
  * (e.g. spark.waimak.apprunner.myapp1.dependencies = myapp2)
  *
  * The [[Env]] implementation used by the provided [[SparkApp]] implementation expects configuration values prefixed with:
  * spark.waimak.environment.{appname}.
  *
  */
object MultiAppRunner {

  implicit class MultiApplicationFlow(flow: SparkDataFlow) {
    def executeApp(dependencies: String*)(app: SparkSession => Any, outputLabel: String): SparkDataFlow = {
      def run(m: DataFlowEntities): ActionResult = {
        val res = app.apply(flow.spark)
        Seq(Some(res))
      }

      flow.addAction(new SimpleAction(dependencies.toList, List(outputLabel), run))
    }
  }

  def main(args: Array[String]): Unit = {
    runAll(SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate())
  }

  def runAll(sparkSession: SparkSession): Unit = {
    val allApps = CaseClassConfigParser[AllApps](SparkFlowContext(sparkSession), "spark.waimak.apprunner.")
    val allAppsConfig = allApps.apps.map(appName => appName ->
      CaseClassConfigParser[SingleAppConfig](SparkFlowContext(sparkSession), s"spark.waimak.apprunner.$appName."))
    val executor = Waimak.sparkExecutor()
    val finalFlow = allAppsConfig.foldLeft(Waimak.sparkFlow(sparkSession))((flow, appConfig) => addAppToFlow(flow, appConfig._1, appConfig._2))
    executor.execute(finalFlow)
  }

  def instantiateApp(appClassName: String): SparkApp[_] = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val module = m.staticModule(appClassName)
    m.reflectModule(module).instance.asInstanceOf[SparkApp[_]]
  }

  def addAppToFlow(flow: SparkDataFlow, appName: String, appConfig: SingleAppConfig): SparkDataFlow = {
    val instantiatedApp = instantiateApp(appConfig.appClassName)
    flow.executeApp(appConfig.dependencies: _*)(
      instantiatedApp.runSparkApp(_, s"spark.waimak.environment.$appName."), appName)
  }

}

case class AllApps(apps: Seq[String])

case class SingleAppConfig(appClassName: String, dependencies: Seq[String] = Nil)
