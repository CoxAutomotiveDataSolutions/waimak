package com.coxautodata.waimak.spark.app

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe.TypeTag

/**
  * During the development lifecycle of Spark applications, it is useful to create sandbox environments comprising paths
  * and Hive databases etc. which are tied to specific logical environments (e.g. dev, test, prod) and feature development
  * (i.e Git branches).
  * e.g. when working on a feature called new_feature for a project called my_project, the application should write its
  * data to paths under /data/dev/my_project/new_feature/ and create tables in a database called dev_my_project_new_feature
  * (actual implementation of what these environments should look like can be defined by extending [[Env]] or one of its
  * subclasses - the final implementation should be a case class whose values define the environment i.e env, branch etc.)
  *
  * This is a generic Spark Application which uses an implementation of [[Env]] to generate application-specific configuration
  * and subsequently parse this configuration into a case class to be used for the application logic.
  *
  * @tparam E the type of the [[Env]] implementation (must be a case class)
  */
abstract class SparkApp[E <: Env : TypeTag] {
  /**
    * Runs the application
    *
    * N.B does not create the environment - use [[createEnv]]
    *
    * @param sparkSession the SparkSession
    * @param envPrefix    the prefix for keys in the SparkConf needed by the [[Env]] implementation
    */
  def runSparkApp(sparkSession: SparkSession, envPrefix: String): Unit = {
    val env = parseEnv(sparkSession, envPrefix)
    runWithEnv(env, sparkSession)
  }

  protected def runWithEnv(env: E, sparkSession: SparkSession): Unit = {
    val defaultConfs = confDefaults(env)
    (defaultConfs ++ sparkSession.conf.getAll.filterKeys(defaultConfs.keySet.contains))
      .foreach(kv => sparkSession.conf.set(kv._1, kv._2))
    run(sparkSession, env)
  }

  /**
    * Create the environment associated with this application
    *
    * @param sparkSession the SparkSession
    * @param envPrefix    the prefix for keys in the SparkConf needed by the [[Env]] implementation
    */
  def createEnv(sparkSession: SparkSession, envPrefix: String): Unit = {
    val env = parseEnv(sparkSession, envPrefix)
    env.create(sparkSession)
  }

  /**
    * Cleans up the environment associated with this application
    *
    * @param sparkSession the SparkSession
    * @param envPrefix    the prefix for keys in the SparkConf needed by the [[Env]] implementation
    */
  def cleanupEnv(sparkSession: SparkSession, envPrefix: String): Unit = {
    val env = parseEnv(sparkSession, envPrefix)
    env.cleanup(sparkSession)
  }

  /**
    * Parses configuration in the SparkSession into the environment case class (type [[E]])
    *
    * @param sparkSession the SparkSession
    * @param envPrefix    the prefix for keys in the SparkConf needed by the [[Env]] implementation
    * @return a parsed case class of type [[E]]
    */
  def parseEnv(sparkSession: SparkSession, envPrefix: String): E = CaseClassConfigParser.fromMap[E](sparkSession.conf.getAll, envPrefix)

  /**
    * Default Spark configuration values to use for the application
    *
    * @param env the environment
    * @return a map containing default Spark configuration
    */
  def confDefaults(env: E): Map[String, String]

  /**
    * Run the application for given environment and configuration case classes
    *
    * @param sparkSession the SparkSession
    * @param env          the environment
    */
  protected def run(sparkSession: SparkSession, env: E): Unit
}
