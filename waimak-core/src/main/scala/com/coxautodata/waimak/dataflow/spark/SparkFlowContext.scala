package com.coxautodata.waimak.dataflow.spark

import java.net.URI

import com.coxautodata.waimak.dataflow.{DataFlowAction, FlowContext}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

/**
  * Context required in a Spark data flow (SparkSession and FileSystem)
  *
  * Created by Vicky Avison on 23/02/2018.
  *
  * @param spark the SparkSession
  */
case class SparkFlowContext(spark: SparkSession) extends FlowContext {

  val uriUsed: String = getString("spark.waimak.fs.defaultFS", spark.sparkContext.hadoopConfiguration.get("fs.defaultFS"))

  lazy val fileSystem: FileSystem = FileSystem.get(new URI(uriUsed), spark.sparkContext.hadoopConfiguration)

  override def setPoolIntoContext(poolName: String): Unit = spark.sparkContext.setLocalProperty("spark.scheduler.pool", poolName)

  override def reportActionStarted(action: DataFlowAction): Unit = spark.sparkContext.setJobGroup(action.guid, action.description)

  override def reportActionFinished(action: DataFlowAction): Unit = spark.sparkContext.clearJobGroup()

  override def getOption(key: String): Option[String] = spark.conf.getOption(key)
}