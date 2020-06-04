package com.coxautodata.waimak.dataflow.spark

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}

/**
  * Created by Vicky Avison on 24/10/17.
  */
trait SparkSpec extends FunSpec with Matchers with BeforeAndAfterEach {

  var sparkSession: SparkSession = _
  val master = "local[2]"
  val appName: String

  /**
   * Allows configuring spark session options. Override this in your own tests to set any
   * config options you might wish to add. This default implementation also detects if you are
   * running the tests on windows, and sets the bindAddress option to make the tests work.
   *
   * @see com.coxautodata.waimak.metastore.TestHiveDBConnector for an example
   *
   * @return a spark session
   */
  def builderOptions: SparkSession.Builder => SparkSession.Builder = { sparkSession =>
    if (SystemUtils.IS_OS_WINDOWS) sparkSession.config("spark.driver.bindAddress", "localhost")
    else sparkSession
  }

  override def beforeEach(): Unit = {
    val preBuilder = SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .config("spark.executor.memory", "2g")
      .config("spark.ui.enabled", "false")

    sparkSession = builderOptions(preBuilder).getOrCreate()
  }

  override def afterEach(): Unit = {
    sparkSession.stop()
  }
}


trait SparkAndTmpDirSpec extends SparkSpec {

  var testingBaseDir: java.nio.file.Path = _
  var testingBaseDirName: String = _
  var tmpDir: Path = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    testingBaseDir = Files.createTempDirectory("test_output")
    testingBaseDirName = testingBaseDir.toString
    tmpDir = new Path(testingBaseDir.toAbsolutePath.toString + "/tmp")
  }

  override def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteDirectory(testingBaseDir.toFile)
  }
}

