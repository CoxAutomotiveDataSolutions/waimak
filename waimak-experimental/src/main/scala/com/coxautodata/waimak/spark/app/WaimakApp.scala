package com.coxautodata.waimak.spark.app

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe.TypeTag

abstract class WaimakApp[E <: Env : TypeTag, C: TypeTag] extends SparkApp[E, C] {

  override protected def run(sparkSession: SparkSession, env: E, confPrefix: String): Unit = {
    val executor = Waimak.sparkExecutor()
    val emptyFlow = Waimak.sparkFlow(sparkSession, env.tmpDir)
    executor.execute(flow(emptyFlow, parseSparkConf(sparkSession, confPrefix)))
  }

  def flow(emptyFlow: SparkDataFlow, conf: C): SparkDataFlow
}
