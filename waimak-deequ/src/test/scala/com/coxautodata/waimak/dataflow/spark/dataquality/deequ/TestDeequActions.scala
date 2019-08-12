package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import java.sql.Timestamp
import java.time.{ZoneOffset, ZonedDateTime}

import com.amazon.deequ.SerializableAnalysisResult
import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.anomalydetection.RateOfChangeStrategy
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import com.coxautodata.waimak.dataflow.spark.dataquality.{TestAlert, TestDataForDataQualityCheck}

import scala.util.Failure

class TestDeequActions extends SparkAndTmpDirSpec {
  override val appName: String = "TestDataQualityActions"

  describe("deequ actions") {
    it("should allow multiple checks to be added for a single label") {
      val spark = sparkSession
      import spark.implicits._
      val ds = Seq(
        TestDataForDataQualityCheck(null, "bla")
        , TestDataForDataQualityCheck(null, "bla")
        , TestDataForDataQualityCheck(null, "bla3")
        , TestDataForDataQualityCheck(null, "bla4")
        , TestDataForDataQualityCheck("a", "bla5")
        , TestDataForDataQualityCheck("b", "bla6")
        , TestDataForDataQualityCheck("c", "bla7")
        , TestDataForDataQualityCheck("d", "bla8")
        , TestDataForDataQualityCheck("e", "bla9")
        , TestDataForDataQualityCheck("f", "bla10")
      ).toDS()
      val alerter = new TestAlert
      val flow = Waimak.sparkFlow(spark, tmpDir.toString)
      flow.addInput("testInput", Some(ds))
        .alias("testInput", "testOutput")
        .addDeequValidation("testOutput",
          _.addChecks(Seq(
            Check(CheckLevel.Warning, "warning_checks")
              .hasCompleteness("col1", _ >= 0.8, Some("More than 20% of col1 values were null."))
          ))
          , alerter
        )
        .addDeequCheck("testOutput", Check(CheckLevel.Error, "error_checks")
          .hasCompleteness("col1", completeness => completeness >= 0.6 && completeness < 0.8, Some("More than 40% of col1 values were null."))
          .isUnique("col2", Some("col2 was not unique"))
        )(alerter)
        .execute()

      alerter.alerts.toList.map(_.alertMessage) should contain theSameElementsAs Seq(
        "Warning alert for label testOutput\n CompletenessConstraint(Completeness(col1,None)) : Value: 0.6 does not meet the constraint requirement! More than 20% of col1 values were null."
        , "Critical alert for label testOutput\n UniquenessConstraint(Uniqueness(List(col2))) : Value: 0.8 does not meet the constraint requirement! col2 was not unique"
      )
    }

    it("should alert when the percentage of nulls increase in a day") {
      val spark = sparkSession
      val alerter1 = new TestAlert
      import spark.implicits._
      val ds1 = Seq(
        TestDataForDataQualityCheck(null, "bla")
        , TestDataForDataQualityCheck("d", "bla8")
        , TestDataForDataQualityCheck("e", "bla9")
        , TestDataForDataQualityCheck("f", "bla10")
      ).toDS()
      val flow = Waimak.sparkFlow(spark, tmpDir.toString)

      flow.addInput("testInput", Some(ds1))
        .alias("testInput", "testOutput")
        .setDeequStorageLayerMetricsRepository(testingBaseDirName + "/metrics", ZonedDateTime.of(2019, 3, 26, 12, 20, 11, 0, ZoneOffset.UTC))
        .addDeequValidationWithMetrics("testOutput",
          _.addAnomalyCheck(RateOfChangeStrategy(maxRateDecrease = Some(0.2)), Completeness("col1"))
          , alerter1
        ).execute()

      alerter1.alerts.toList.map(_.alertMessage) should contain theSameElementsAs List("Warning alert for label testOutput\n AnomalyConstraint(Completeness(col1,None)) : Can't execute the assertion: requirement failed: There have to be previous results in the MetricsRepository!!")

      val alerter2 = new TestAlert
      spark.read.parquet(testingBaseDirName + "/metrics/testOutput").show(false)
      val ds2 = Seq(
        TestDataForDataQualityCheck("a", "bla")
        , TestDataForDataQualityCheck("d", "bla8")
        , TestDataForDataQualityCheck("e", "bla9")
        , TestDataForDataQualityCheck("f", "bla10")
      ).toDS()
      flow.addInput("testInput", Some(ds2))
        .alias("testInput", "testOutput")
        .setDeequStorageLayerMetricsRepository(testingBaseDirName + "/metrics", ZonedDateTime.of(2019, 3, 27, 12, 20, 11, 0, ZoneOffset.UTC))
        .addDeequValidationWithMetrics("testOutput",
          _.addAnomalyCheck(RateOfChangeStrategy(maxRateDecrease = Some(0.2)), Completeness("col1"))
          , alerter2
        ).execute()
      spark.read.parquet(testingBaseDirName + "/metrics/testOutput").show(false)

      alerter2.alerts.toList.map(_.alertMessage) should contain theSameElementsAs List()

      val alerter3 = new TestAlert
      val ds3 = Seq(
        TestDataForDataQualityCheck("a", "bla")
        , TestDataForDataQualityCheck("d", "bla8")
        , TestDataForDataQualityCheck(null, "bla9")
        , TestDataForDataQualityCheck(null, "bla10")
      ).toDS()
      flow.addInput("testInput", Some(ds3))
        .alias("testInput", "testOutput")
        .setDeequStorageLayerMetricsRepository(testingBaseDirName + "/metrics", ZonedDateTime.of(2019, 3, 28, 12, 20, 11, 0, ZoneOffset.UTC))
        .addDeequValidationWithMetrics("testOutput",
          _.addAnomalyCheck(RateOfChangeStrategy(maxRateDecrease = Some(0.4)), Completeness("col1"))
          , alerter3
        ).execute()

      alerter3.alerts.toList.map(_.alertMessage) should contain theSameElementsAs List(
        "Warning alert for label testOutput\n AnomalyConstraint(Completeness(col1,None)) : Value: 0.5 does not meet the constraint requirement!"
      )

     spark.read.parquet(s"$testingBaseDirName/metrics").as[SerializableAnalysisResult].collect().toList should contain theSameElementsAs Seq(
        SerializableAnalysisResult(List(), Timestamp.valueOf("2019-03-26 12:20:11.0"), "[\n  {\n    \"resultKey\": {\n      \"dataSetDate\": 1553602811000,\n      \"tags\": {}\n    },\n    \"analyzerContext\": {\n      \"metricMap\": [\n        {\n          \"analyzer\": {\n            \"analyzerName\": \"Completeness\",\n            \"column\": \"col1\"\n          },\n          \"metric\": {\n            \"metricName\": \"DoubleMetric\",\n            \"entity\": \"Column\",\n            \"instance\": \"col1\",\n            \"name\": \"Completeness\",\n            \"value\": 0.75\n          }\n        }\n      ]\n    }\n  }\n]")
     , SerializableAnalysisResult(List(), Timestamp.valueOf("2019-03-27 12:20:11.0"), "[\n  {\n    \"resultKey\": {\n      \"dataSetDate\": 1553689211000,\n      \"tags\": {}\n    },\n    \"analyzerContext\": {\n      \"metricMap\": [\n        {\n          \"analyzer\": {\n            \"analyzerName\": \"Completeness\",\n            \"column\": \"col1\"\n          },\n          \"metric\": {\n            \"metricName\": \"DoubleMetric\",\n            \"entity\": \"Column\",\n            \"instance\": \"col1\",\n            \"name\": \"Completeness\",\n            \"value\": 1.0\n          }\n        }\n      ]\n    }\n  }\n]")
     , SerializableAnalysisResult(List(), Timestamp.valueOf("2019-03-28 12:20:11.0"), "[\n  {\n    \"resultKey\": {\n      \"dataSetDate\": 1553775611000,\n      \"tags\": {}\n    },\n    \"analyzerContext\": {\n      \"metricMap\": [\n        {\n          \"analyzer\": {\n            \"analyzerName\": \"Completeness\",\n            \"column\": \"col1\"\n          },\n          \"metric\": {\n            \"metricName\": \"DoubleMetric\",\n            \"entity\": \"Column\",\n            \"instance\": \"col1\",\n            \"name\": \"Completeness\",\n            \"value\": 0.5\n          }\n        }\n      ]\n    }\n  }\n]")
      )

    }

    it("should fail if there is validation with metrics when no metrics repository has been set") {
      val spark = sparkSession
      val alerter = new TestAlert
      import spark.implicits._
      val ds1 = Seq(
        TestDataForDataQualityCheck(null, "bla")
        , TestDataForDataQualityCheck("d", "bla8")
        , TestDataForDataQualityCheck("e", "bla9")
        , TestDataForDataQualityCheck("f", "bla10")
      ).toDS()
      val flow = Waimak.sparkFlow(spark, tmpDir.toString)

      val f = flow.addInput("testInput", Some(ds1))
        .alias("testInput", "testOutput")
        //.setDeequStorageLayerMetricsRepository(testingBaseDirName + "/metrics", ZonedDateTime.of(2019, 3, 26, 12, 20, 11, 0, ZoneOffset.UTC))
        .addDeequValidationWithMetrics("testOutput",
        _.addAnomalyCheck(RateOfChangeStrategy(maxRateDecrease = Some(0.2)), Completeness("col1"))
        , alerter
      )
      val ex = intercept[DeequCheckException] {
        f.execute()
      }

      ex.getMessage should be("Anomaly checks were specified but no metrics repository was set, or metrics repository was set after anomaly checks were defined. " +
        "Use setDeequMetricsRepository or setDeequStorageLayerMetricsRepository to set a repository and ensure you set the repository before calling any checks that need it.")

      f.prepareForExecution() should be(Failure(DeequCheckException("Anomaly checks were specified but no metrics repository was set, or metrics repository was set after anomaly checks were defined. " +
        "Use setDeequMetricsRepository or setDeequStorageLayerMetricsRepository to set a repository and ensure you set the repository before calling any checks that need it.")))
    }
  }

}