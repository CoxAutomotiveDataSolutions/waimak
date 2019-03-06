package com.coxautodata.waimak.spark.app

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe.TypeTag

abstract class SparkApp[E <: Env : TypeTag, C: TypeTag] {
  def runSparkApp(sparkSession: SparkSession, envPrefix: String, confPrefix: String): Unit = {
    val env = parseEnv(sparkSession, envPrefix)
    runWithEnv(env, sparkSession, confPrefix)
  }

  protected def runWithEnv(env: E, sparkSession: SparkSession, confPrefix: String): Unit = {
    val defaultConfs = confDefaults(env, confPrefix)
    (defaultConfs ++ sparkSession.conf.getAll.filterKeys(defaultConfs.keySet.contains))
      .foreach(kv => sparkSession.conf.set(kv._1, kv._2))
    run(sparkSession, env, confPrefix)
  }

  def createEnv(sparkSession: SparkSession, envPrefix: String): Unit = {
    val env = parseEnv(sparkSession, envPrefix)
    env.create(sparkSession)
  }

  def cleanupEnv(sparkSession: SparkSession, envPrefix: String): Unit = {
    val env = parseEnv(sparkSession, envPrefix)
    env.cleanup(sparkSession)
  }

  def parseEnv(spark: SparkSession, envPrefix: String): E = CaseClassConfigParser.fromMap[E](spark.conf.getAll, envPrefix)

  def confDefaults(env: E, confPrefix: String): Map[String, String]

  def parseSparkConf(sparkSession: SparkSession, confPrefix: String): C = CaseClassConfigParser.fromMap[C](sparkSession.conf.getAll, confPrefix)

  protected def run(sparkSession: SparkSession, env: E, confPrefix: String): Unit
}