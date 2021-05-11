package com.coxautodata.waimak.spark.app

import java.nio.file.Files
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

trait AppRunnerSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  var testingBaseDir: java.nio.file.Path = _
  var testingBaseDirName: String = _
  var tmpDir: Path = _
  var sparkSession: SparkSession = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    testingBaseDir = Files.createTempDirectory("test_output")
    testingBaseDirName = testingBaseDir.toString
    tmpDir = new Path(testingBaseDir.toAbsolutePath.toString + "/tmp")
  }

  override def afterEach(): Unit = {
    super.afterEach()
    sparkSession.stop()
    FileUtils.deleteDirectory(testingBaseDir.toFile)
  }

  def appName: String

  def buildSparkSession(extraConfig: Map[String, String]): SparkSession =
    extraConfig.foldLeft(SparkSession
      .builder()
      .appName(appName)
      .master("local[2]")
      .config("spark.executor.memory", "2g")
      .config("spark.ui.enabled", "false"))((session, kv) => {
      session.config(kv._1, kv._2)
    }).getOrCreate()

}