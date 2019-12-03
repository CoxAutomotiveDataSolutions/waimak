package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID

import com.coxautodata.waimak.dataflow.spark.dataquality.{DataQualityAlertException, TestAlert, TestDataForDataQualityCheck}
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
        TestDataForDataQualityCheck("01", "bla")
        , TestDataForDataQualityCheck("02", "bla")
        , TestDataForDataQualityCheck("03", "bla3")
        , TestDataForDataQualityCheck("04", "bla4")
        , TestDataForDataQualityCheck("05", "bla5")
        , TestDataForDataQualityCheck("06", null)
        , TestDataForDataQualityCheck("07", null)
        , TestDataForDataQualityCheck("08", null)
        , TestDataForDataQualityCheck("09", null)
        , TestDataForDataQualityCheck("10", null)
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

  describe("UniquenessCheck") {
    def getFlow(_sparkSession: SparkSession, columns: Seq[String] = Seq("col1")): SparkDataFlow = {
      val spark = _sparkSession

      import spark.implicits._

      spark.conf.set("spark.waimak.dataflow.extensions", "deequ")
      spark.conf.set("spark.waimak.dataquality.alerters", "test,exception")
      spark.conf.set("spark.waimak.dataquality.alerters.test.alertOn", "warning,critical")
      spark.conf.set("spark.waimak.dataquality.alerters.test.uuid", UUID.randomUUID().toString)
      spark.conf.set("spark.waimak.dataquality.alerters.exception.alertOn", "critical")
      spark.conf.set("spark.waimak.dataquality.deequ.labelsToMonitor", "testOutput")
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.checks", "uniquenessCheck")
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.uniquenessCheck.columns", columns.mkString(","))

      val ds = Seq(
        TestDataForDataQualityCheck("01", "bla")
        , TestDataForDataQualityCheck("02", "bla")
        , TestDataForDataQualityCheck("03", "bla3")
        , TestDataForDataQualityCheck("04", "bla4")
        , TestDataForDataQualityCheck("05", "bla5")
        , TestDataForDataQualityCheck("06", "bla5")
        , TestDataForDataQualityCheck("07", "bla5")
        , TestDataForDataQualityCheck("08", "bla5")
        , TestDataForDataQualityCheck("09", "bla5")
        , TestDataForDataQualityCheck("09", "bla5")
      ).toDS()

      Waimak
        .sparkFlow(spark, tmpDir.toString)
        .addInput("testInput", Some(ds))
        .transform("testInput")("testOutput")(identity)
    }

    it("should not trigger any alerts if the column is unique") {
      val spark = sparkSession
      import spark.implicits._

      getFlow(sparkSession)
        .inPlaceTransform("testOutput")(_.filter('col1 =!= "09"))
        .execute()

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List()
    }

    it("should trigger a warning alert if the column is not unique") {
      val spark = sparkSession

      getFlow(sparkSession)
        .execute()

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List(
        "Warning alert for label testOutput\n UniquenessConstraint(Uniqueness(List(col1))) : Value: 0.8 does not meet the constraint requirement! col1 was not 100.0% unique."
      )
    }

    it("should not trigger an alert if the column has uniqueness over the threshold") {
      val spark = sparkSession

      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.uniquenessCheck.warningThreshold", "0.8")

      getFlow(sparkSession)
        .execute()

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List()
    }

    it("should trigger an exception if the column has uniqueness under the critical threshold") {
      val spark = sparkSession

      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.uniquenessCheck.warningThreshold", "0.95")

      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.uniquenessCheck.criticalThreshold", "0.9")

      val cause = intercept[DataFlowException] {
        getFlow(sparkSession)
          .execute()
      }.cause

      cause shouldBe a[DataQualityAlertException]
      cause.asInstanceOf[DataQualityAlertException].text should be(
        "Critical: Critical alert for label testOutput\n UniquenessConstraint(Uniqueness(List(col1))) : Value: 0.8 does not meet the constraint requirement! col1 was not 90.0% unique.")


      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List(
        "Critical alert for label testOutput\n UniquenessConstraint(Uniqueness(List(col1))) : Value: 0.8 does not meet the constraint requirement! col1 was not 90.0% unique."
        , "Warning alert for label testOutput\n UniquenessConstraint(Uniqueness(List(col1))) : Value: 0.8 does not meet the constraint requirement! col1 was not 95.0% unique."
      )
    }

    it("should work for a combination of columns") {
      val spark = sparkSession

      getFlow(sparkSession, Seq("col1", "col2"))
        .execute()

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List(
        "Warning alert for label testOutput\n UniquenessConstraint(Uniqueness(List(col1, col2))) : Value: 0.8 does not meet the constraint requirement! col1,col2 was not 100.0% unique."
      )
    }
  }

  describe("GenericSQLCheck") {
    def getFlow(_sparkSession: SparkSession, warningCheck: String = "col1 <= 08"): SparkDataFlow = {
      val spark = _sparkSession

      import spark.implicits._

      spark.conf.set("spark.waimak.dataflow.extensions", "deequ")
      spark.conf.set("spark.waimak.dataquality.alerters", "test,exception")
      spark.conf.set("spark.waimak.dataquality.alerters.test.alertOn", "warning,critical")
      spark.conf.set("spark.waimak.dataquality.alerters.test.uuid", UUID.randomUUID().toString)
      spark.conf.set("spark.waimak.dataquality.alerters.exception.alertOn", "critical")
      spark.conf.set("spark.waimak.dataquality.deequ.labelsToMonitor", "testOutput")
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.checks", "genericSQLCheck")
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.genericSQLCheck.warningChecks", warningCheck)

      val ds = Seq(
        TestDataForDataQualityCheck("01", "bla")
        , TestDataForDataQualityCheck("02", "bla")
        , TestDataForDataQualityCheck("03", "bla3")
        , TestDataForDataQualityCheck("04", "bla4")
        , TestDataForDataQualityCheck("05", "bla5")
        , TestDataForDataQualityCheck("06", null)
        , TestDataForDataQualityCheck("07", null)
        , TestDataForDataQualityCheck("08", null)
        , TestDataForDataQualityCheck("09", null)
        , TestDataForDataQualityCheck("09", null)
      ).toDS()

      Waimak
        .sparkFlow(spark, tmpDir.toString)
        .addInput("testInput", Some(ds))
        .transform("testInput")("testOutput")(identity)
    }

    it("should not trigger any alerts if the condition is satisfied") {
      val spark = sparkSession
      import spark.implicits._

      getFlow(sparkSession)
        .inPlaceTransform("testOutput")(_.filter('col1 =!= "09"))
        .execute()

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List()
    }

    it("should trigger alerts if the condition is not satisfied") {
      val spark = sparkSession
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.genericSQLCheck.criticalChecks", "length(col2)=4;col2 is not null")

      val cause = intercept[DataFlowException] {
        getFlow(sparkSession)
          .execute()
      }.cause

      cause shouldBe a[DataQualityAlertException]

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs Seq(
        "Warning alert for label testOutput\n ComplianceConstraint(Compliance(generic sql constraint,col1 <= 08,None)) : Value: 0.8 does not meet the constraint requirement!"
        , "Critical alert for label testOutput\n ComplianceConstraint(Compliance(generic sql constraint,col2 is not null,None)) : Value: 0.5 does not meet the constraint requirement!"
        , "Critical alert for label testOutput\n ComplianceConstraint(Compliance(generic sql constraint,length(col2)=4,None)) : Value: 0.3 does not meet the constraint requirement!"
      )
    }

    it("should work with checks which have commas in them") {
      val spark = sparkSession
      getFlow(sparkSession, warningCheck = "col1 in ('01', '02')")
        .execute()

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs Seq(
        "Warning alert for label testOutput\n ComplianceConstraint(Compliance(generic sql constraint,col1 in ('01', '02'),None)) : Value: 0.2 does not meet the constraint requirement!"
      )
    }

  }

  describe("RecentTimestampCheck") {

    val now = LocalDateTime.now()
    val nowTimestamp = Timestamp.valueOf(now)
    val twoHoursAgo = Timestamp.valueOf(now.minusHours(2))
    val sixHoursAgo = Timestamp.valueOf(now.minusHours(6))
    val sevenHoursAgo = Timestamp.valueOf(now.minusHours(7))

    def getFlow(_sparkSession: SparkSession): SparkDataFlow = {
      val spark = _sparkSession

      import spark.implicits._

      spark.conf.set("spark.waimak.dataflow.extensions", "deequ")
      spark.conf.set("spark.waimak.dataquality.alerters", "test,exception")
      spark.conf.set("spark.waimak.dataquality.alerters.test.alertOn", "warning,critical")
      spark.conf.set("spark.waimak.dataquality.alerters.test.uuid", UUID.randomUUID().toString)
      spark.conf.set("spark.waimak.dataquality.alerters.exception.alertOn", "critical")
      spark.conf.set("spark.waimak.dataquality.deequ.labelsToMonitor", "testOutput")
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.checks", "recentTimestampCheck")
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.recentTimestampCheck.column", "ts")

      val ds = Seq(
        TestDataForTimestampDeequCheck(1, twoHoursAgo)
        , TestDataForTimestampDeequCheck(2, sevenHoursAgo)
      ).toDS()

      Waimak
        .sparkFlow(spark, tmpDir.toString)
        .addInput("testInput", Some(ds))
        .transform("testInput")("testOutput")(identity)
    }

    it("should not trigger any alerts if there is a recent timestamp") {
      val spark = sparkSession

      getFlow(sparkSession)
        .execute()

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List()
    }

    it("should trigger an alert if there is no recent timestamp") {
      val spark = sparkSession
      import spark.implicits._


      //Force current timestamp for testing
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.recentTimestampCheck.nowOverride", nowTimestamp.toString)

      getFlow(sparkSession)
        .inPlaceTransform("testOutput")(_.filter('id =!= 1))
        .execute()

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List(
        s"Warning alert for label testOutput\n SizeConstraint(Size(Some(ts >= '${sixHoursAgo}'))) : Value: 0 does not meet the constraint requirement! No new data in the last 6 hours."
      )
    }

    it("should not trigger an alert if there is a timestamp within the configured interval") {
      val spark = sparkSession
      import spark.implicits._

      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.recentTimestampCheck.hoursToLookBack", "8")

      getFlow(sparkSession)
        .inPlaceTransform("testOutput")(_.filter('id =!= 1))
        .execute()

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List()
    }

    it("should throw an exception alert if configured") {
      val spark = sparkSession
      import spark.implicits._


      //Force current timestamp for testing
      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.recentTimestampCheck.nowOverride", nowTimestamp.toString)

      spark.conf.set("spark.waimak.dataquality.deequ.labels.testOutput.recentTimestampCheck.alertLevel", "critical")

      val cause = intercept[DataFlowException] {
        getFlow(sparkSession)
          .inPlaceTransform("testOutput")(_.filter('id =!= 1))
          .execute()
      }.cause

      cause shouldBe a[DataQualityAlertException]

      cause.asInstanceOf[DataQualityAlertException].text should be(s"Critical: Critical alert for label testOutput\n SizeConstraint(Size(Some(ts >= '$sixHoursAgo'))) : Value: 0 does not meet the constraint requirement! No new data in the last 6 hours.")

      val alerterUUID = UUID.fromString(spark.conf.get("spark.waimak.dataquality.alerters.test.uuid"))
      TestAlert.getAlerts(alerterUUID).map(_.alertMessage) should contain theSameElementsAs List(
        s"Critical alert for label testOutput\n SizeConstraint(Size(Some(ts >= '$sixHoursAgo'))) : Value: 0 does not meet the constraint requirement! No new data in the last 6 hours."
      )
    }
  }

}

case class TestDataForTimestampDeequCheck(id: Int, ts: Timestamp)
