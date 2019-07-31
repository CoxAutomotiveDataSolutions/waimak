package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import java.util.UUID

import com.coxautodata.waimak.dataflow.spark.dataquality.{DataQualityAlertException, TestAlert, TestDataForNullsCheck}
import com.coxautodata.waimak.dataflow.spark.{SparkAndTmpDirSpec, SparkDataFlow}
import com.coxautodata.waimak.dataflow.{DataFlowException, Waimak}
import org.apache.spark.sql.SparkSession

class TestDeequPrefabChecks extends SparkAndTmpDirSpec {
  override val appName: String = "TestDeequPrefabChecks"

  describe("CompletenessCheck") {

    def getFlow(_sparkSession: SparkSession): SparkDataFlow = {
      val spark = _sparkSession

      import spark.implicits._

      spark.conf.set("spark.waimak.dataflow.extensions", "deequ")
      spark.conf.set("spark.waimak.dataquality.alerters", "test,exception")
      spark.conf.set("spark.waimak.dataquality.alerters.test.alertOn", "warning,critical")
      spark.conf.set("spark.waimak.dataquality.alerters.test.uuid", UUID.randomUUID().toString)
      spark.conf.set("spark.waimak.dataquality.alerters.exception.alertOn", "critical")
      spark.conf.set("spark.waimak.dataquality.deequ.labelsToMonitor", "testOutput")
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.checks", "completenessCheck")
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.completenessCheck.columns", "col2")
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.completenessCheck.warningThreshold", "0.8")
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.completenessCheck.criticalThreshold", "0.6")

      val ds = Seq(
        TestDataForNullsCheck("01", "bla")
        , TestDataForNullsCheck("02", "bla")
        , TestDataForNullsCheck("03", "bla3")
        , TestDataForNullsCheck("04", "bla4")
        , TestDataForNullsCheck("05", "bla5")
        , TestDataForNullsCheck("06", null)
        , TestDataForNullsCheck("07", null)
        , TestDataForNullsCheck("08", null)
        , TestDataForNullsCheck("09", null)
        , TestDataForNullsCheck("10", null)
      ).toDS()

      Waimak
        .sparkFlow(spark, tmpDir.toString)
        .addInput("testInput", Some(ds))
        .transform("testInput")("testOutput")(identity)
    }

    it("should not trigger any alerts") {
      val spark = sparkSession
      import spark.implicits._

      getFlow(sparkSession)
        .inPlaceTransform("testOutput")(_.filter('col1 <= "05"))
        .execute()

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List()

    }

    it("should trigger warning alert") {

      val spark = sparkSession
      import spark.implicits._

      getFlow(sparkSession)
        .inPlaceTransform("testOutput")(_.filter('col1 <= "08"))
        .execute()

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List("Warning alert for label testOutput\n CompletenessConstraint(Completeness(col2,None)) : Value: 0.625 does not meet the constraint requirement! Less than 80.0% of col2 values were complete.")
    }

    it("should trigger exception alert") {
      val cause = intercept[DataFlowException] {
        getFlow(sparkSession)
          .execute()
      }.cause
      cause shouldBe a[DataQualityAlertException]
      cause.asInstanceOf[DataQualityAlertException].text should be("Critical: Critical alert for label testOutput\n CompletenessConstraint(Completeness(col2,None)) : Value: 0.5 does not meet the constraint requirement! Less than 60.0% of col2 values were complete.")

      val alerterUUID = UUID.fromString(sparkSession.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List(
        "Warning alert for label testOutput\n CompletenessConstraint(Completeness(col2,None)) : Value: 0.5 does not meet the constraint requirement! Less than 80.0% of col2 values were complete.",
        "Critical alert for label testOutput\n CompletenessConstraint(Completeness(col2,None)) : Value: 0.5 does not meet the constraint requirement! Less than 60.0% of col2 values were complete.")
    }

  }

}
