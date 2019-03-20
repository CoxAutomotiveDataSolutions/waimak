package com.coxautodata.waimak.spark.app

import org.apache.hadoop.fs.{FileSystem, Path}

class TestEnvironmentManager extends AppRunnerSpec {

  override def appName: String = "TestEnvironmentManager"

  describe("performEnvironmentAction") {
    it("should create a Hive environment") {
      sparkSession = buildSparkSession(Map(
        "spark.waimak.environment.appClassName" -> "com.coxautodata.waimak.spark.app.WaimakAppWithNoDependency"
        , "spark.waimak.environment.action" -> "create"
        , "spark.waimak.environment.project" -> "app_1"
        , "spark.waimak.environment.environment" -> "dev"
        , "spark.waimak.environment.branch" -> "feature/test-multi-app-runner"
        , "spark.waimak.environment.uri" -> testingBaseDirName
      ))
      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      val basePath = s"$testingBaseDirName/data/dev/app_1/feature_test_multi_app_runner"
      val dbName = "dev_app_1_feature_test_multi_app_runner"
      fs.exists(new Path(basePath)) should be(false)
      sparkSession.catalog.databaseExists(dbName) should be(false)
      EnvironmentManager.performEnvironmentAction(sparkSession)
      fs.exists(new Path(basePath)) should be(true)
      sparkSession.catalog.databaseExists(dbName) should be(true)
    }

    it("should cleanup a Hive environment") {
      sparkSession = buildSparkSession(Map(
        "spark.waimak.environment.appClassName" -> "com.coxautodata.waimak.spark.app.WaimakAppWithNoDependency"
        , "spark.waimak.environment.action" -> "cleanup"
        , "spark.waimak.environment.project" -> "app_1"
        , "spark.waimak.environment.environment" -> "dev"
        , "spark.waimak.environment.branch" -> "feature/test-multi-app-runner"
        , "spark.waimak.environment.uri" -> testingBaseDirName
      ))
      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      val basePath = s"$testingBaseDirName/data/dev/app_1/feature_test_multi_app_runner"
      val dbName = "dev_app_1_feature_test_multi_app_runner"
      fs.mkdirs(new Path(basePath))
      sparkSession.sql(s"create database $dbName")
      fs.exists(new Path(basePath)) should be(true)
      sparkSession.catalog.databaseExists(dbName) should be(true)
      EnvironmentManager.performEnvironmentAction(sparkSession)
      fs.exists(new Path(basePath)) should be(false)
      sparkSession.catalog.databaseExists(dbName) should be(false)
    }

    it("should throw an UnsupportedOperationException if the action is neither create nor cleanup") {
      sparkSession = buildSparkSession(Map(
        "spark.waimak.environment.appClassName" -> "com.coxautodata.waimak.spark.app.WaimakAppWithNoDependency"
        , "spark.waimak.environment.action" -> "not-a-real-environment-action"
        , "spark.waimak.environment.project" -> "app_1"
        , "spark.waimak.environment.environment" -> "dev"
        , "spark.waimak.environment.branch" -> "feature/test-multi-app-runner"
        , "spark.waimak.environment.uri" -> testingBaseDirName
      ))
      val err = intercept[UnsupportedOperationException](EnvironmentManager.performEnvironmentAction(sparkSession))
      err.getMessage should be("Unsupported environment action: not-a-real-environment-action")
    }
  }
}
