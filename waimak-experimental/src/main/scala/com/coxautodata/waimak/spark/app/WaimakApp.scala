package com.coxautodata.waimak.spark.app

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe.TypeTag

/**
  * This is a [[SparkApp]] specifically for applications using Waimak
  *
  * @tparam E the type of the [[Env]] implementation (must be a case class)
  * @tparam C the type of case class to use for the app configuration (values will be parsed from the SparkSession)
  */
abstract class WaimakApp[E <: Env : TypeTag, C: TypeTag] extends SparkApp[E, C] {

  override protected def run(sparkSession: SparkSession, env: E, config: C): Unit = {
    val executor = Waimak.sparkExecutor()
    val emptyFlow = Waimak.sparkFlow(sparkSession, env.tmpDir)
    executor.execute(flow(emptyFlow, config))
  }

  def flow(emptyFlow: SparkDataFlow, conf: C): SparkDataFlow
}

