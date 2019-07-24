package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import java.time.{ZoneOffset, ZonedDateTime}

import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.anomalydetection.RateOfChangeStrategy
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import com.coxautodata.waimak.dataflow.spark.dataquality.{DataQualityAlert, DataQualityAlertHandler}

import scala.collection.mutable

class TestDeequActions extends SparkAndTmpDirSpec {
  override val appName: String = "TestDataQualityActions"

  describe("nulls percentage metric") {
    it("should alert when the percentage of nulls exceeds a threshold") {
      val spark = sparkSession
      import spark.implicits._
      val ds = Seq(
        TestDataForNullsCheck(null, "bla")
        , TestDataForNullsCheck(null, "bla")
        , TestDataForNullsCheck(null, "bla3")
        , TestDataForNullsCheck(null, "bla4")
        , TestDataForNullsCheck("a", "bla5")
        , TestDataForNullsCheck("b", "bla6")
        , TestDataForNullsCheck("c", "bla7")
        , TestDataForNullsCheck("d", "bla8")
        , TestDataForNullsCheck("e", "bla9")
        , TestDataForNullsCheck("f", "bla10")
      ).toDS()
      val flow = Waimak.sparkFlow(spark, tmpDir.toString)
      val f = flow.addInput("testInput", Some(ds))
        .alias("testInput", "testOutput")
        .addDeequValidation("testOutput",
          _.addChecks(Seq(
            Check(CheckLevel.Warning, "warning_checks")
              .hasCompleteness("col1", _ >= 0.8, Some("More than 20% of col1 values were null."))
            , Check(CheckLevel.Error, "error_checks")
              .hasCompleteness("col1", completeness => completeness >= 0.6 && completeness < 0.8, Some("More than 40% of col1 values were null."))
              .isUnique("col2", Some("col2 was not unique"))
          ))
          , new TestAlert
        )
      Waimak.sparkExecutor().execute(f)
    }

    it("should alert when the percentage of nulls increase in a day") {
      val spark = sparkSession
      val alerter = new TestAlert
      import spark.implicits._
      val ds1 = Seq(
        TestDataForNullsCheck(null, "bla")
        , TestDataForNullsCheck("d", "bla8")
        , TestDataForNullsCheck("e", "bla9")
        , TestDataForNullsCheck("f", "bla10")
      ).toDS()
      val flow = Waimak.sparkFlow(spark, tmpDir.toString)

      flow.addInput("testInput", Some(ds1))
        .alias("testInput", "testOutput")
        .setDeequStorageLayerMetricsRepository(testingBaseDirName + "/metrics", ZonedDateTime.of(2019, 3, 26, 12, 20, 11, 0, ZoneOffset.UTC))
        .addDeequValidationWithMetrics("testOutput",
          _.addAnomalyCheck(RateOfChangeStrategy(maxRateDecrease = Some(0.2)), Completeness("col1"))
          , alerter
        ).execute()

      alerter.alerts.toList.map(_.alertMessage) should contain theSameElementsAs List("Warning alert for label testOutput\n AnomalyConstraint(Completeness(col1,None)) : Can't execute the assertion: requirement failed: There have to be previous results in the MetricsRepository!!\n       ")

      val ds2 = Seq(
        TestDataForNullsCheck("a", "bla")
        , TestDataForNullsCheck("d", "bla8")
        , TestDataForNullsCheck("e", "bla9")
        , TestDataForNullsCheck("f", "bla10")
      ).toDS()
      flow.addInput("testInput", Some(ds2))
        .alias("testInput", "testOutput")
        .setDeequStorageLayerMetricsRepository(testingBaseDirName + "/metrics", ZonedDateTime.of(2019, 3, 27, 12, 20, 11, 0, ZoneOffset.UTC))
        .addDeequValidationWithMetrics("testOutput",
          _.addAnomalyCheck(RateOfChangeStrategy(maxRateDecrease = Some(0.2)), Completeness("col1"))
          , alerter
        ).execute()

      alerter.alerts.toList.map(_.alertMessage) should contain theSameElementsAs List("Warning alert for label testOutput\n AnomalyConstraint(Completeness(col1,None)) : Can't execute the assertion: requirement failed: There have to be previous results in the MetricsRepository!!\n       ")


      val ds3 = Seq(
        TestDataForNullsCheck("a", "bla")
        , TestDataForNullsCheck("d", "bla8")
        , TestDataForNullsCheck(null, "bla9")
        , TestDataForNullsCheck(null, "bla10")
      ).toDS()
      flow.addInput("testInput", Some(ds3))
        .alias("testInput", "testOutput")
        .setDeequStorageLayerMetricsRepository(testingBaseDirName + "/metrics", ZonedDateTime.of(2019, 3, 28, 12, 20, 11, 0, ZoneOffset.UTC))
        .addDeequValidationWithMetrics("testOutput",
          _.addAnomalyCheck(RateOfChangeStrategy(maxRateDecrease = Some(0.4)), Completeness("col1"))
          , alerter
        ).execute()

      alerter.alerts.toList.map(_.alertMessage) should contain theSameElementsAs List(
        "Warning alert for label testOutput\n AnomalyConstraint(Completeness(col1,None)) : Can't execute the assertion: requirement failed: There have to be previous results in the MetricsRepository!!\n       ",
        "Warning alert for label testOutput\n AnomalyConstraint(Completeness(col1,None)) : Value: 0.5 does not meet the constraint requirement!\n       "
      )

    }
  }

}

case class TestDataForNullsCheck(col1: String, col2: String)

case class TestDataForUniqueIDsCheck(idCol: Int, col2: String)

class TestAlert extends DataQualityAlertHandler {

  val alerts: mutable.ListBuffer[DataQualityAlert] = mutable.ListBuffer()

  override def handleAlert(alert: DataQualityAlert): Unit = {
    alerts.append(alert)
    println(alert)
  }
}

