package com.coxautodata.waimak.dataflow.spark

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

/**
  * Context required in a Spark data flow (SparkSession and FileSystem)
  *
  * Created by Vicky Avison on 23/02/2018.
  *
  * @param spark the SparkSession
  */
case class SparkFlowContext(spark: SparkSession) {
  //TODO: explore initialisation with non Hadoop files systems
  lazy val fileSystem: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
}

