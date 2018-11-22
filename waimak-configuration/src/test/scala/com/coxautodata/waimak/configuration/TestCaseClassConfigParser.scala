package com.coxautodata.waimak.configuration

import java.sql.Timestamp
import java.util.Properties

import com.coxautodata.waimak.configuration.CaseClassConfigParser.separator
import org.apache.spark.SparkConf
import org.scalatest.{FunSpec, Matchers}

object TestCaseClasses {

  case class PrimTypesTest(string: String, byte: Byte, short: Short, int: Int, long: Long, float: Float, double: Double, boolean: Boolean)

  case class OptionPrimTypesTest(string: Option[String], byte: Option[Byte], short: Option[Short], int: Option[Int],
                                 long: Option[Long], float: Option[Float], double: Option[Double], boolean: Option[Boolean])

  case class DefArgTest(boolean: Boolean = false, optionInt: Option[Int] = None)

  case class Pref1Test(boolean: Boolean = false, optionInt: Option[Int] = None)

  case class Pref2Test(boolean: Boolean = false, optionInt: Option[Int] = None)

  case class SeqTest(string: Seq[String], int: Seq[Int])

  case class ListTest(string: List[String])

  case class ListSeqVectorTest(list: List[String], seq: Seq[Double], vector: Vector[Boolean])

  case class SeqSeparatorTest(string: Seq[String], @separator("==") int: Seq[Int])

  case class MissingArgTest(missing: String)

  case class ParseErrorIntTest(intError: Int)

  case class ParseErrorOptionIntTest(optionIntError: Option[Int])

  case class ParseErrorBooleanTest(booleanError: Boolean)

  case class ParseUnknownTypeTest(unknown: Timestamp)

  case class ParseUnsupportedCollectionTest(unsupported: Array[String])

  case class PropertiesTest(string: String, optionString: Option[String] = None)

}

class TestCaseClassConfigParser extends FunSpec with Matchers {

  import TestCaseClasses._

  describe("Successful configuration") {

    it("parse a case class with all supported primitive types") {
      val conf: SparkConf = new SparkConf()
      conf.set("string", "string")
      conf.set("byte", "100")
      conf.set("short", "1")
      conf.set("int", "2")
      conf.set("long", "3")
      conf.set("float", "4.1")
      conf.set("double", "5.2")
      conf.set("boolean", "true")
      CaseClassConfigParser[PrimTypesTest](conf, "") should be(PrimTypesTest("string", 100.toByte, 1, 2, 3, 4.1f, 5.2, true))
    }

    it("parse a case class with all supported primitive option types set with values") {
      val conf: SparkConf = new SparkConf()
      conf.set("string", "string")
      conf.set("byte", "100")
      conf.set("short", "1")
      conf.set("int", "2")
      conf.set("long", "3")
      conf.set("float", "4.1")
      conf.set("double", "5.2")
      conf.set("boolean", "true")
      CaseClassConfigParser[OptionPrimTypesTest](conf, "") should be(OptionPrimTypesTest(Some("string"), Some(100.toByte),
        Some(1), Some(2), Some(3), Some(4.1f), Some(5.2), Some(true)))
    }

    it("parse a case class with default arguments set and having no config set in SparkConf") {
      val conf: SparkConf = new SparkConf()
      CaseClassConfigParser[DefArgTest](conf, "") should be(DefArgTest(false, None))
    }

    it("parse a case class with default arguments set and config set in SparkConf") {
      val conf: SparkConf = new SparkConf()
      conf.set("boolean", "true")
      conf.set("optionInt", "1")
      CaseClassConfigParser[DefArgTest](conf, "") should be(DefArgTest(true, Some(1)))
    }

    it("parse two case classes with different prefixes") {
      val conf: SparkConf = new SparkConf()
      conf.set("spark.pref1.boolean", "true")
      conf.set("spark.pref2.optionInt", "1")
      CaseClassConfigParser[Pref1Test](conf, "spark.pref1.") should be(Pref1Test(true, None))
      CaseClassConfigParser[Pref2Test](conf, "spark.pref2.") should be(Pref2Test(false, Some(1)))
    }

    it("parse a list of strings and ints") {
      val conf: SparkConf = new SparkConf()
      conf.set("string", "one,two,three")
      conf.set("int", "1")
      CaseClassConfigParser[SeqTest](conf, "") should be(SeqTest(Seq("one", "two", "three"), Seq(1)))
    }

    it("parse a list of strings and ints with custom separator") {
      val conf: SparkConf = new SparkConf()
      conf.set("string", "one,two,three")
      conf.set("int", "1==2==3")
      CaseClassConfigParser[SeqSeparatorTest](conf, "") should be(SeqSeparatorTest(Seq("one", "two", "three"), Seq(1, 2, 3)))
    }

    it("parse a list of strings and ints with custom and string value as empty") {
      val conf: SparkConf = new SparkConf()
      conf.set("string", "")
      conf.set("int", "1")
      CaseClassConfigParser[SeqSeparatorTest](conf, "") should be(SeqSeparatorTest(Seq(""), Seq(1)))
    }

    it("parse a list type") {
      val conf: SparkConf = new SparkConf()
      conf.set("string", "")
      CaseClassConfigParser[ListTest](conf, "") should be(ListTest(List("")))
    }

    it("parse all supported collection types") {
      val conf: SparkConf = new SparkConf()
      conf.set("list", "one,two,three")
      conf.set("seq", "1.1,2.2,3.3")
      conf.set("vector", "true,false")
      CaseClassConfigParser[ListSeqVectorTest](conf, "") should be(
        ListSeqVectorTest(List("one", "two", "three"), Seq(1.1, 2.2, 3.3), Vector(true, false))
      )
    }

    it("get parameters from properties file when it is defined") {
      val conf: SparkConf = new SparkConf()
      val prop: Properties = new Properties()
      prop.setProperty("test.string", "test1")
      CaseClassConfigParser[PropertiesTest](conf, "test.", Some(prop)) should be(PropertiesTest("test1"))
    }

  }

  describe("Exception configurations") {

    it("parsing a missing configuration should throw an exception") {
      val conf: SparkConf = new SparkConf()
      intercept[NoSuchElementException] {
        CaseClassConfigParser[MissingArgTest](conf, "")
      }
    }

    it("parsing a configuration with a wrong prefix should throw an exception") {
      val conf: SparkConf = new SparkConf()
      conf.set("spark.missing", "value")
      // Check for false positive result
      CaseClassConfigParser[MissingArgTest](conf, "spark.") should be(MissingArgTest("value"))
      intercept[NoSuchElementException] {
        CaseClassConfigParser[MissingArgTest](conf, "")
      }
    }

    it("parsing a configuration with wrong type should throw a parsing error") {
      val conf: SparkConf = new SparkConf()
      conf.set("intError", "wrong")
      conf.set("optionIntError", "wrong")
      conf.set("booleanError", "wrong")
      intercept[NumberFormatException] {
        CaseClassConfigParser[ParseErrorIntTest](conf, "")
      }
      intercept[NumberFormatException] {
        CaseClassConfigParser[ParseErrorOptionIntTest](conf, "")
      }
      intercept[IllegalArgumentException] {
        CaseClassConfigParser[ParseErrorBooleanTest](conf, "")
      }
    }

    it("parsing a case class with an unsupported type should throw an exception") {
      val conf: SparkConf = new SparkConf()
      conf.set("unknown", "wrong")
      intercept[UnsupportedOperationException] {
        CaseClassConfigParser[ParseUnknownTypeTest](conf, "")
      }
    }

    it("parsing a case class nested in a class should throw an exception with a helpful error") {
      val conf: SparkConf = new SparkConf()
      intercept[UnsupportedOperationException] {
        CaseClassConfigParser[ParseNestedClassTest](conf, "")
      }
    }

    it("parsing a case class with an unsupported collection type should throw an exception") {
      val conf: SparkConf = new SparkConf()
      conf.set("unsupported", "1,2")
      intercept[UnsupportedOperationException] {
        CaseClassConfigParser[ParseUnsupportedCollectionTest](conf, "")
      }
    }

  }

  case class ParseNestedClassTest(string: String = "")

}
