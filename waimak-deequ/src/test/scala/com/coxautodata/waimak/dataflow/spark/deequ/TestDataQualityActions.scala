package com.coxautodata.waimak.dataflow.spark.deequ

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import com.coxautodata.waimak.dataflow.spark.deequ.DataQualityActions._

class TestDataQualityActions extends SparkAndTmpDirSpec {
  override val appName: String = "TestDataQualityActions"

  describe("nulls percentage metric") {
    it("should alert when the percentage of nulls exceeds a threshold") {
      val spark = sparkSession
      import spark.implicits._
      val ds = Seq(
        TestDataForNullsCheck(null, "bla")
        , TestDataForNullsCheck(null, "bla2")
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
              .hasCompleteness("col1", _ >= 0.8, Some("extra info"))
            , Check(CheckLevel.Error, "error_checks")
              .hasCompleteness("col1", completeness => completeness >= 0.6 && completeness < 0.8, Some("extra info"))
              .isUnique("col2")
          ))
        )
      Waimak.sparkExecutor().execute(f)
    }
  }

}

case class TestDataForNullsCheck(col1: String, col2: String)

case class TestDataForUniqueIDsCheck(idCol: Int, col2: String)

