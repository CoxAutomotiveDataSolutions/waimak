package com.coxautodata.waimak.spark.app

import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.dataflow.spark.SparkDataFlow

class TestMultiAppRunner extends AppRunnerSpec {

  override def appName: String = "TestMultiAppRUnner"

  describe("runAll") {
    it("should run a single application") {
      sparkSession = buildSparkSession(Map(
        "spark.waimak.apprunner.apps" -> "no_dependency_app"
        , "spark.waimak.apprunner.no_dependency_app.appClassName" -> "com.coxautodata.waimak.spark.app.WaimakAppWithNoDependency"
        , "spark.waimak.environment.no_dependency_app.project" -> "app_1"
        , "spark.waimak.environment.no_dependency_app.environment" -> "dev"
        , "spark.waimak.environment.no_dependency_app.branch" -> "feature/test-multi-app-runner"
        , "spark.waimak.environment.no_dependency_app.uri" -> testingBaseDirName
      ))
      val spark = sparkSession
      import spark.implicits._
      MultiAppRunner.runAll(spark)
      spark.read.parquet(s"$testingBaseDirName/data/dev/app_1/feature_test_multi_app_runner/output/test")
        .as[String].collect() should contain theSameElementsAs Seq(
        "test1"
        , "test2"
      )
    }

    it("should run two applications with no dependencies") {
      sparkSession = buildSparkSession(Map(
        "spark.waimak.apprunner.apps" -> "no_dependency_app_1,no_dependency_app_2"
        , "spark.waimak.apprunner.no_dependency_app_1.appClassName" -> "com.coxautodata.waimak.spark.app.WaimakAppWithNoDependency"
        , "spark.waimak.apprunner.no_dependency_app_2.appClassName" -> "com.coxautodata.waimak.spark.app.WaimakAppWithNoDependency"
        , "spark.waimak.environment.no_dependency_app_1.project" -> "app_1"
        , "spark.waimak.environment.no_dependency_app_1.environment" -> "dev"
        , "spark.waimak.environment.no_dependency_app_1.branch" -> "feature/test-multi-app-runner"
        , "spark.waimak.environment.no_dependency_app_1.uri" -> testingBaseDirName
        , "spark.waimak.environment.no_dependency_app_2.project" -> "app_2"
        , "spark.waimak.environment.no_dependency_app_2.environment" -> "dev"
        , "spark.waimak.environment.no_dependency_app_2.branch" -> "feature/another-test"
        , "spark.waimak.environment.no_dependency_app_2.uri" -> testingBaseDirName
      ))
      val spark = sparkSession
      import spark.implicits._
      MultiAppRunner.runAll(spark)
      spark.read.parquet(s"$testingBaseDirName/data/dev/app_1/feature_test_multi_app_runner/output/test")
        .as[String].collect() should contain theSameElementsAs Seq(
        "test1"
        , "test2"
      )
      spark.read.parquet(s"$testingBaseDirName/data/dev/app_2/feature_another_test/output/test")
        .as[String].collect() should contain theSameElementsAs Seq(
        "test1"
        , "test2"
      )
    }

    it("should run two applications with a dependency") {
      sparkSession = buildSparkSession(Map(
        "spark.waimak.apprunner.apps" -> "no_dependency_app,dependency_app"
        , "spark.waimak.apprunner.no_dependency_app.appClassName" -> "com.coxautodata.waimak.spark.app.WaimakAppWithNoDependency"
        , "spark.waimak.apprunner.dependency_app.appClassName" -> "com.coxautodata.waimak.spark.app.WaimakAppWithDependency"
        , "spark.waimak.apprunner.dependency_app.dependencies" -> "no_dependency_app"
        , "spark.waimak.environment.no_dependency_app.project" -> "app_1"
        , "spark.waimak.environment.no_dependency_app.environment" -> "dev"
        , "spark.waimak.environment.no_dependency_app.branch" -> "feature/test-multi-app-runner"
        , "spark.waimak.environment.no_dependency_app.uri" -> testingBaseDirName
        , "spark.waimak.environment.dependency_app.project" -> "app_2"
        , "spark.waimak.environment.dependency_app.environment" -> "dev"
        , "spark.waimak.environment.dependency_app.branch" -> "feature/another-test"
        , "spark.waimak.environment.dependency_app.uri" -> testingBaseDirName
        , "spark.dependency_app.inputPath" -> s"$testingBaseDirName/data/dev/app_1/feature_test_multi_app_runner/output"
      ))
      val spark = sparkSession
      import spark.implicits._
      MultiAppRunner.runAll(spark)
      spark.read.parquet(s"$testingBaseDirName/data/dev/app_1/feature_test_multi_app_runner/output/test")
        .as[String].collect() should contain theSameElementsAs Seq(
        "test1"
        , "test2"
      )
      spark.read.parquet(s"$testingBaseDirName/data/dev/app_2/feature_another_test/output/test_transformed")
        .as[String].collect() should contain theSameElementsAs Seq(
        "test1_new"
        , "test2_new"
      )
    }
  }
}


object WaimakAppWithNoDependency extends WaimakApp[TestEnv, WaimakAppNoDependencyConf] {

  override def flow(emptyFlow: SparkDataFlow, conf: WaimakAppNoDependencyConf): SparkDataFlow = {
    import emptyFlow.flowContext.spark.implicits._
    emptyFlow
      .addInput("test", Some(Seq("test1", "test2").toDF("col_1")))
      .writeParquet(conf.outputPath)("test")
  }

  override def confDefaults(env: TestEnv, confPrefix: String): Map[String, String] = Map(
    s"${confPrefix}outputPath" -> s"${env.basePath}/output"
  )
}

object WaimakAppWithDependency extends WaimakApp[TestEnv, WaimakAppWithDependencyConf] {

  import org.apache.spark.sql.functions._

  override def flow(emptyFlow: SparkDataFlow, conf: WaimakAppWithDependencyConf): SparkDataFlow = {
    import emptyFlow.flowContext.spark.implicits._
    emptyFlow
      .openParquet(conf.inputPath)("test")
      .transform("test")("test_transformed")(_.withColumn("col_1", concat($"col_1", lit("_new"))))
      .writeParquet(conf.outputPath)("test_transformed")
  }

  override def confDefaults(env: TestEnv, confPrefix: String): Map[String, String] = Map(
    s"${confPrefix}outputPath" -> s"${env.basePath}/output"
  )
}

case class WaimakAppNoDependencyConf(outputPath: String)

case class WaimakAppWithDependencyConf(inputPath: String, outputPath: String)

case class TestEnv(project: String
                   , environment: String
                   , branch: String
                   , uri: String) extends HiveEnv
