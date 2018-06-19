package com.coxautodata.waimak.configuration

import java.io.InputStreamReader
import java.util.Properties

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
  * Created by abush on 22/11/17.
  */
object SecureCredentialUtils {

  def getPropertiesFile(file: String, spark: SparkSession, charsetName: String = "UTF-8"): Either[Throwable, Properties] = Try {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val config: Properties = new Properties()
    val stream = fs.open(new Path(file))
    val stream2 = new InputStreamReader(stream, charsetName)
    config.load(stream2)
    stream2.close()
    stream.close()
    config
  } match {
    case Success(p) => Right(p)
    case Failure(e) => Left(e)
  }

}
