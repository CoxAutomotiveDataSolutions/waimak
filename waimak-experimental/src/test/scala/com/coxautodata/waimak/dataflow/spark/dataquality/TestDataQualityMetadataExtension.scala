package com.coxautodata.waimak.dataflow.spark.dataquality

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable

class TestDataQualityMetadataExtension extends SparkAndTmpDirSpec {
  override val appName: String = "TestDataQualityMetadataExtension"

  describe("data quality actions") {
    it("allow multiple actions to be added for the same label") {
      val spark = sparkSession
      import spark.implicits._
      val alerter = new TestAlert
      val ds = Seq(
        TestDataForNullsCheck(null, null)
        , TestDataForNullsCheck(null, null)
        , TestDataForNullsCheck(null, null)
        , TestDataForNullsCheck(null, null)
        , TestDataForNullsCheck("a", null)
        , TestDataForNullsCheck("b", null)
        , TestDataForNullsCheck("c", "bla7")
        , TestDataForNullsCheck("d", "bla8")
        , TestDataForNullsCheck("e", "bla9")
        , TestDataForNullsCheck("f", "bla10")
      ).toDS()
      val flow = Waimak.sparkFlow(spark, tmpDir.toString)
      flow.addInput("testInput", Some(ds))
        .alias("testInput", "testOutput")
        .addDataQualityCheck("testOutput"
        , DatasetChecks(Seq(NullValuesCheck("col1", 20, 40)))
        , alerter)
        .addDataQualityCheck("testOutput"
        , DatasetChecks(Seq(NullValuesCheck("col2", 20, 40)))
        , alerter)
        .execute()

      alerter.alerts.map(_.alertMessage) should contain theSameElementsAs Seq(
        "Warning alert for null_values on label testOutput. Percentage of nulls in column col1 was 40%. Warning threshold 20%"
        , "Critical alert for null_values on label testOutput. Percentage of nulls in column col2 was 60%. Critical threshold 40%"
      )
    }
  }

}

case class NullValuesCheck(colName: String, percentageNullWarningThreshold: Int, percentageNullCriticalThreshold: Int)
  extends SimpleDatasetCheck[Int](df => {
    import df.sparkSession.implicits._
    df.withColumn("nulls_count", sum(when($"$colName".isNull, 1).otherwise(0)).over(Window.partitionBy()))
      .withColumn("total_count", count("*").over(Window.partitionBy()))
      .withColumn("perc_nulls", (($"nulls_count" / $"total_count") * 100).cast(IntegerType))
      .select("perc_nulls")
      .as[Int]
  }
    , (ds, label) => {
      ds.collect().headOption.filter(_ > percentageNullWarningThreshold.min(percentageNullCriticalThreshold)).map(perc => {
        val (alertImportance, thresholdUsed) = perc match {
          case p if p > percentageNullCriticalThreshold => (Critical, percentageNullCriticalThreshold)
          case _ => (Warning, percentageNullWarningThreshold)
        }
        Seq(DataQualityAlert(s"${alertImportance.description} alert for null_values on label $label. Percentage of nulls in column $colName was $perc%. " +
          s"${alertImportance.description} threshold $thresholdUsed%", alertImportance))
      }).getOrElse(Nil)
    })


case class TestDataForNullsCheck(col1: String, col2: String)

class TestAlert extends DataQualityAlertHandler {

  val alerts: mutable.ListBuffer[DataQualityAlert] = mutable.ListBuffer()

  override def handleAlert(alert: DataQualityAlert): Unit = {
    alerts.append(alert)
  }

  override def alertOn: List[AlertImportance] = List.empty
}

