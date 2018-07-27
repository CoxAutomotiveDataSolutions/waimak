package com.coxautodata.waimak.azure.table

import java.sql.{Date, Timestamp}

import com.coxautodata.waimak.azure.table.PropertyEncoder.encoderType
import com.coxautodata.waimak.dataflow.spark.SparkSpec

case class SupportedProperties(maybeBoolean: Option[Boolean],
                               maybeByte: Option[Byte],
                               maybeDate: Option[Date],
                               maybeDecimal: Option[BigDecimal],
                               maybeDouble: Option[Double],
                               maybeFloat: Option[Float],
                               maybeString: Option[String],
                               maybeInteger: Option[Int],
                               maybeLong: Option[Long],
                               maybeShort: Option[Short],
                               maybeTimestamp: Option[Timestamp])

case class UnsupportedProperty(maybeBinary: Option[Array[Byte]],
                               maybeBoolean: Option[Boolean])

class TestPropertyEncoder extends SparkSpec {

  override val appName: String = "PropertyEncoder"

  describe("toEncoder") {

    it("parse a dataframe with all supported property types") {
      val spark = sparkSession
      import spark.implicits._
      val emptyDS = spark.emptyDataset[SupportedProperties]

      val encoder = PropertyEncoder.toEncoder(emptyDS.schema, Set.empty)

      encoder.size should be(11)
    }

    it("parse a dataframe with an unsupported property and throw an exception") {
      val spark = sparkSession
      import spark.implicits._
      val emptyDS = spark.emptyDataset[UnsupportedProperty]

      val res = intercept[RuntimeException](PropertyEncoder.toEncoder(emptyDS.schema, Set.empty))

      res.getMessage should be("Unknown type conversion for Azure Table: binary")
    }

    it("parse a dataframe with an unsupported property but ignore if it is in the ignore list") {
      val spark = sparkSession
      import spark.implicits._
      val emptyDS = spark.emptyDataset[UnsupportedProperty]

      val encoder = PropertyEncoder.toEncoder(emptyDS.schema, Set("maybeBinary"))

      encoder.size should be(1)
    }
  }

  describe("encodedTypes") {
    it("should give null values for all None fields") {
      val spark = sparkSession
      import spark.implicits._
      val ds = List(SupportedProperties(None, None, None, None, None, None, None, None, None, None, None)).toDS()
      val encoder: Map[String, encoderType] = PropertyEncoder.toEncoder(ds.schema, Set.empty)
      val row = ds.toDF().head()

      val maybeBooleanEncoded = encoder("maybeBoolean")(row)
      maybeBooleanEncoded.getIsNull should be(true)
      maybeBooleanEncoded.getType should be(classOf[java.lang.Boolean])

      val maybeByteEncoded = encoder("maybeByte")(row)
      maybeByteEncoded.getIsNull should be(true)
      maybeByteEncoded.getType should be(classOf[java.lang.Integer])

      val maybeDateEncoded = encoder("maybeDate")(row)
      maybeDateEncoded.getIsNull should be(true)
      maybeDateEncoded.getType should be(classOf[java.util.Date])

      val maybeDecimalEncoded = encoder("maybeDecimal")(row)
      maybeDecimalEncoded.getIsNull should be(true)
      maybeDecimalEncoded.getType should be(classOf[java.lang.Double])

      val maybeDoubleEncoded = encoder("maybeDouble")(row)
      maybeDoubleEncoded.getIsNull should be(true)
      maybeDoubleEncoded.getType should be(classOf[java.lang.Double])

      val maybeFloatEncoded = encoder("maybeFloat")(row)
      maybeFloatEncoded.getIsNull should be(true)
      maybeFloatEncoded.getType should be(classOf[java.lang.Double])

      val maybeStringEncoded = encoder("maybeString")(row)
      maybeStringEncoded.getIsNull should be(true)
      maybeStringEncoded.getType should be(classOf[java.lang.String])

      val maybeIntEncoded = encoder("maybeInteger")(row)
      maybeIntEncoded.getIsNull should be(true)
      maybeIntEncoded.getType should be(classOf[java.lang.Integer])

      val maybeLongEncoded = encoder("maybeLong")(row)
      maybeLongEncoded.getIsNull should be(true)
      maybeLongEncoded.getType should be(classOf[java.lang.Long])

      val maybeShortEncoded = encoder("maybeShort")(row)
      maybeShortEncoded.getIsNull should be(true)
      maybeShortEncoded.getType should be(classOf[java.lang.Integer])

      val maybeTimestampEncoded = encoder("maybeTimestamp")(row)
      maybeTimestampEncoded.getIsNull should be(true)
      maybeTimestampEncoded.getType should be(classOf[java.util.Date])
    }

    it("should give values for all fields") {
      val spark = sparkSession
      import spark.implicits._
      val ds = List(
        SupportedProperties(
          Some(false),
          Some(9),
          Some(new Date(2018, 7, 25)),
          Some(BigDecimal(1.5)),
          Some(2.5),
          Some(3.5f),
          Some("test"),
          Some(1),
          Some(2),
          Some(3),
          Some(new Timestamp(1532535940317L)))
      ).toDS()
      val encoder: Map[String, encoderType] = PropertyEncoder.toEncoder(ds.schema, Set.empty)
      val row = ds.toDF().head()

      val maybeBooleanEncoded = encoder("maybeBoolean")(row)
      maybeBooleanEncoded.getValueAsBooleanObject should be(false: java.lang.Boolean)
      maybeBooleanEncoded.getType should be(classOf[java.lang.Boolean])

      val maybeByteEncoded = encoder("maybeByte")(row)
      maybeByteEncoded.getValueAsIntegerObject should be(9)
      maybeByteEncoded.getType should be(classOf[java.lang.Integer])

      val maybeDateEncoded = encoder("maybeDate")(row)
      maybeDateEncoded.getValueAsDate.toInstant should be(new java.util.Date(2018, 7, 25).toInstant)
      maybeDateEncoded.getType should be(classOf[java.util.Date])

      val maybeDecimalEncoded = encoder("maybeDecimal")(row)
      maybeDecimalEncoded.getValueAsDoubleObject should be(1.5)
      maybeDecimalEncoded.getType should be(classOf[java.lang.Double])

      val maybeDoubleEncoded = encoder("maybeDouble")(row)
      maybeDoubleEncoded.getValueAsDoubleObject should be(2.5)
      maybeDoubleEncoded.getType should be(classOf[java.lang.Double])

      val maybeFloatEncoded = encoder("maybeFloat")(row)
      maybeFloatEncoded.getValueAsDoubleObject should be(3.5)
      maybeFloatEncoded.getType should be(classOf[java.lang.Double])

      val maybeStringEncoded = encoder("maybeString")(row)
      maybeStringEncoded.getValueAsString should be("test")
      maybeStringEncoded.getType should be(classOf[java.lang.String])

      val maybeIntEncoded = encoder("maybeInteger")(row)
      maybeIntEncoded.getValueAsIntegerObject should be(1)
      maybeIntEncoded.getType should be(classOf[java.lang.Integer])

      val maybeLongEncoded = encoder("maybeLong")(row)
      maybeLongEncoded.getValueAsLongObject should be(2)
      maybeLongEncoded.getType should be(classOf[java.lang.Long])

      val maybeShortEncoded = encoder("maybeShort")(row)
      maybeShortEncoded.getValueAsIntegerObject should be(3)
      maybeShortEncoded.getType should be(classOf[java.lang.Integer])

      val maybeTimestampEncoded = encoder("maybeTimestamp")(row)
      maybeTimestampEncoded.getValueAsDate.getTime should be(1532535940317L)
      maybeTimestampEncoded.getType should be(classOf[java.util.Date])
    }
  }
}
