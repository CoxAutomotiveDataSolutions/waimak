package com.coxautodata.waimak.dataflow.spark.TestDataQualityActions

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark._

class TestDataQualityActions extends SparkAndTmpDirSpec {
  override val appName: String = "TestDataQualityActions"

  describe("nulls percentage metric") {
    it("should alert when the percentage of nulls exceeds a threshold") {
//      val spark = sparkSession
//      import spark.implicits._
//      val ds = Seq(
//        TestDataForNullsCheck(null, "bla")
//        , TestDataForNullsCheck(null, "bla2")
//        , TestDataForNullsCheck(null, "bla3")
//        , TestDataForNullsCheck(null, "bla4")
//        , TestDataForNullsCheck("a", "bla5")
//        , TestDataForNullsCheck("b", "bla6")
//        , TestDataForNullsCheck("c", "bla7")
//        , TestDataForNullsCheck("d", "bla8")
//        , TestDataForNullsCheck("e", "bla9")
//        , TestDataForNullsCheck("f", "bla10")
//      ).toDS()
//      val flow = Waimak.sparkFlow(spark, tmpDir.toString)
//      val f = flow.addInput("testInput", Some(ds))
//        .alias("testInput", "testOutput")
//        .monitor("testOutput")(NullValuesRule("col1", 20, 30))(TestAlertHandler)(StorageLayerMetricStorage(testingBaseDirName, LocalDateTime.now(ZoneId.of("Europe/London"))))
//      Waimak.sparkExecutor().execute(f)
    }

    it("should alert when count of unique ids since a given timestamp is below a given threshold") {
//      val spark = sparkSession
//      import spark.implicits._
//      val minTs = Timestamp.valueOf(LocalDateTime.now())
//      val ds1 = Seq(
//        TestDataForUniqueIDsCheck(1, "a")
//        , TestDataForUniqueIDsCheck(2, "b")
//      ).toDS()
//      val flow = Waimak.sparkFlow(spark, tmpDir.toString)
//      val f1 = flow.addInput("testInput", Some(ds1))
//        .alias("testInput", "testOutput")
//        .monitor("testOutput")(UniqueIDsRule[Long]("idCol", 4, minTs))(TestAlertHandler)(StorageLayerMetricStorage(testingBaseDirName, LocalDateTime.now(ZoneId.of("Europe/London"))))
//      Waimak.sparkExecutor().execute(f1)
//
//      val ds2 = Seq(
//        TestDataForUniqueIDsCheck(1, "c")
//        , TestDataForUniqueIDsCheck(3, "b")
//      ).toDS()
//
//      val f2 = flow.addInput("testInput", Some(ds2))
//        .alias("testInput", "testOutput")
//        .monitor("testOutput")(UniqueIDsRule[Long]("idCol", 4, minTs))(TestAlertHandler)(StorageLayerMetricStorage(testingBaseDirName, LocalDateTime.now(ZoneId.of("Europe/London"))))
//      Waimak.sparkExecutor().execute(f2)
//
//      val ds3 = Seq(
//        TestDataForUniqueIDsCheck(4, "d")
//      ).toDS()
//
//      val f3 = flow.addInput("testInput", Some(ds3))
//        .alias("testInput", "testOutput")
//        .monitor("testOutput")(UniqueIDsRule[Long]("idCol", 4, minTs))(TestAlertHandler)(StorageLayerMetricStorage(testingBaseDirName, LocalDateTime.now(ZoneId.of("Europe/London"))))
//      Waimak.sparkExecutor().execute(f3)
//
//      val f4 = flow.addInput("testInput", Some(ds3))
//        .alias("testInput", "testOutput")
//        .monitor("testOutput")(UniqueIDsRule[Long]("idCol", 4, Timestamp.valueOf(LocalDateTime.now())))(TestAlertHandler)(StorageLayerMetricStorage(testingBaseDirName, LocalDateTime.now(ZoneId.of("Europe/London"))))
//      Waimak.sparkExecutor().execute(f4)
    }
  }
}

case object TestAlertHandler extends DataQualityAlertHandler {
  override def handleAlert(alert: DataQualityAlert): Unit = {
    //do nothing
  }
}

case class TestDataForNullsCheck(col1: String, col2: String)

case class TestDataForUniqueIDsCheck(idCol: Int, col2: String)
