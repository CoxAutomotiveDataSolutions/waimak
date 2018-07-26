package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.dataflow.spark.SparkSpec
import org.apache.spark.sql.Dataset

/**
  * Created by Vicky Avison on 02/07/18.
  */
class TestDataFlowEntities extends SparkSpec {

  var entities: DataFlowEntities = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    entities = DataFlowEntities(Map("a" -> Some("not_an_integer"), "b" -> Some(3), "c" -> Some(sparkSession.emptyDataFrame), "d" -> None))
  }

  describe("get") {

    it("should return the value if it is there with the correct type") {
      entities.get[Int]("b") should be(3)
      entities.get[Dataset[_]]("c") should be(sparkSession.emptyDataFrame)
    }

    it("should throw an exception if the entity is not of the requested type") {
      val res1 = intercept[DataFlowException] {
        entities.get[Int]("a")
      }
      res1.text should be("Label a not of requested type int. Actual type: String")

      val res2 = intercept[DataFlowException] {
        entities.get[Dataset[_]]("a")
      }
      res2.text should be("Label a not of requested type class org.apache.spark.sql.Dataset. Actual type: String")
    }

    it("should throw an exception if the entity is undefined") {
      val res = intercept[DataFlowException] {
        entities.get[String]("d")
      }
      res.text should be("Entity d is undefined")
    }

    it("should throw an exception if the label does not exist") {
      val res = intercept[DataFlowException] {
        entities.get[String]("k")
      }
      res.text should be("Label k does not exist")
    }
  }

  describe("getOption") {
    it("should return the value if it is there with the correct type") {
      entities.getOption[Int]("b") should be(Some(3))
      entities.getOption[Dataset[_]]("c") should be(Some(sparkSession.emptyDataFrame))
    }

    it("should throw an exception if the entity is not of the requested type") {
      val res1 = intercept[DataFlowException] {
        entities.getOption[Int]("a")
      }
      res1.text should be("Label a not of requested type int. Actual type: String")

      val res2 = intercept[DataFlowException] {
        entities.getOption[Dataset[_]]("a")
      }
      res2.text should be("Label a not of requested type class org.apache.spark.sql.Dataset. Actual type: String")
    }

    it("should return None if the entity is undefined") {
      entities.getOption[String]("d") should be(None)
    }

    it("should throw an exception if the label does not exist") {
      val res = intercept[DataFlowException] {
        entities.getOption[String]("k")
      }
      res.text should be("Label k does not exist")
    }
  }

  describe("collect and filter") {
    it("collect should collect all dataset types") {

      val res = entities.collect { case e@(_, Some(_: Dataset[_])) => e }
      res should be(Seq(("c", Some(sparkSession.emptyDataFrame))))

    }

    it("filterValues should filter only Some values") {

      val res = entities.filterValues(_.isDefined)
      res should be(DataFlowEntities(Map("a" -> Some("not_an_integer"), "b" -> Some(3), "c" -> Some(sparkSession.emptyDataFrame))))

    }

    it("filterKeys should filter out a single key") {

      val res = entities.filterLabels(entities.labels.filterNot(_ == "d").toList)
      res should be(DataFlowEntities(Map("a" -> Some("not_an_integer"), "b" -> Some(3), "c" -> Some(sparkSession.emptyDataFrame))))

    }
  }

  override val appName: String = "TestDataFlowEntities"
}
