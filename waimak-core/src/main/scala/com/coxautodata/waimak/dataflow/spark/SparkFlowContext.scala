package com.coxautodata.waimak.dataflow.spark

import java.net.URI
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

  private val uriToUse = spark.conf.get("spark.waimak.fs.defaultFS", spark.sparkContext.hadoopConfiguration.get("fs.defaultFS"))

  lazy val fileSystem: FileSystem = FileSystem.get(new URI(uriToUse), spark.sparkContext.hadoopConfiguration)

}

