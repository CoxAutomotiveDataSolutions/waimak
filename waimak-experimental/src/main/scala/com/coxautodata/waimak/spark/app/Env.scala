package com.coxautodata.waimak.spark.app

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

trait Env {

  def create(sparkSession: SparkSession): Unit

  def cleanup(sparkSession: SparkSession): Unit

  def tmpDir: String

  def normaliseName(name: String): String = name.toLowerCase.replaceAll("[^a-z0-9_]", "_")
}


trait BaseEnv extends Env {

  def uri: String

  def environment: String

  def normalisedEnvironment: String = normaliseName(environment)

  def project: String

  def normalisedProject: String = normaliseName(project)

  def branch: String

  def normalisedBranch: String = normaliseName(branch)

  def basePath: String = normalisedEnvironment match {
    case "prod" => s"$uri/data/prod/$normalisedProject"
    case _ => s"$uri/data/$normalisedEnvironment/$normalisedProject/$normalisedBranch"
  }

  override def tmpDir = s"$basePath/tmp"

  override def create(sparkSession: SparkSession): Unit = {
    println("Creating paths")
    val fs = FileSystem.get(new URI(uri), sparkSession.sparkContext.hadoopConfiguration)
    fs.mkdirs(new Path(basePath))
  }

  override def cleanup(sparkSession: SparkSession): Unit = {
    val fs = FileSystem.get(new URI(uri), sparkSession.sparkContext.hadoopConfiguration)
    fs.delete(new Path(basePath), true)
  }
}

trait HiveEnv extends BaseEnv {

  def baseDBName: String = normalisedEnvironment match {
    case "prod" => s"prod_$normalisedProject"
    case _ => s"${normalisedEnvironment}_${normalisedProject}_$normalisedBranch"
  }

  def extraDBs: Seq[String] = Seq.empty

  def extraDBsNormalised: Seq[String] = extraDBs map normaliseName

  def createBaseDB: Boolean = true

  def allDBs: Seq[String] = if (createBaseDB) {
    extraDBsNormalised.map(n => s"${baseDBName}_$n") :+ baseDBName
  } else extraDBsNormalised.map(n => s"${baseDBName}_$n")

  override def create(sparkSession: SparkSession): Unit = {
    super.create(sparkSession)
    println("Creating dbs")
    allDBs.foreach(dbName => sparkSession.sql(s"create database $dbName"))
  }

  override def cleanup(sparkSession: SparkSession): Unit = {
    super.cleanup(sparkSession)
    allDBs.foreach(dbName => sparkSession.sql(s"drop database $dbName cascade"))
  }
}
