package com.coxautodata.waimak.configuration

import java.sql.Timestamp
import java.util.Properties

import com.coxautodata.waimak.configuration.CaseClassConfigParser._
import com.coxautodata.waimak.dataflow.spark.{SparkFlowContext, SparkSpec}
import org.apache.spark.sql.RuntimeConfig

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

class TestCaseClassConfigParser extends SparkSpec {

  override val appName: String = "TestCaseClassConfigParser"

  import TestCaseClasses._

  describe("Successful configuration") {

    it("parse a case class with all supported primitive types") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("string", "string")
      conf.set("byte", "100")
      conf.set("short", "1")
      conf.set("int", "2")
      conf.set("long", "3")
      conf.set("float", "4.1")
      conf.set("double", "5.2")
      conf.set("boolean", "true")
      CaseClassConfigParser[PrimTypesTest](context, "") should be(PrimTypesTest("string", 100.toByte, 1, 2, 3, 4.1f, 5.2, true))
    }

    it("parse a case class with all supported primitive option types set with values") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("string", "string")
      conf.set("byte", "100")
      conf.set("short", "1")
      conf.set("int", "2")
      conf.set("long", "3")
      conf.set("float", "4.1")
      conf.set("double", "5.2")
      conf.set("boolean", "true")
      CaseClassConfigParser[OptionPrimTypesTest](context, "") should be(OptionPrimTypesTest(Some("string"), Some(100.toByte),
        Some(1), Some(2), Some(3), Some(4.1f), Some(5.2), Some(true)))
    }

    it("parse a case class with default arguments set and having no config set in SparkConf") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      CaseClassConfigParser[DefArgTest](context, "") should be(DefArgTest(false, None))
    }

    it("parse a case class with default arguments set and config set in SparkConf") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("boolean", "true")
      conf.set("optionInt", "1")
      CaseClassConfigParser[DefArgTest](context, "") should be(DefArgTest(true, Some(1)))
    }

    it("parse two case classes with different prefixes") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("spark.pref1.boolean", "true")
      conf.set("spark.pref2.optionInt", "1")
      CaseClassConfigParser[Pref1Test](context, "spark.pref1.") should be(Pref1Test(true, None))
      CaseClassConfigParser[Pref2Test](context, "spark.pref2.") should be(Pref2Test(false, Some(1)))
    }

    it("parse a list of strings and ints") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("string", "one,two,three")
      conf.set("int", "1")
      CaseClassConfigParser[SeqTest](context, "") should be(SeqTest(Seq("one", "two", "three"), Seq(1)))
    }

    it("parse a list of strings and ints with custom separator") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("string", "one,two,three")
      conf.set("int", "1==2==3")
      CaseClassConfigParser[SeqSeparatorTest](context, "") should be(SeqSeparatorTest(Seq("one", "two", "three"), Seq(1, 2, 3)))
    }

    it("parse a list of strings and ints with custom and string value as empty") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("string", "")
      conf.set("int", "1")
      CaseClassConfigParser[SeqSeparatorTest](context, "") should be(SeqSeparatorTest(Seq(""), Seq(1)))
    }

    it("parse a list type") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("string", "")
      CaseClassConfigParser[ListTest](context, "") should be(ListTest(List("")))
    }

    it("parse all supported collection types") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("list", "one,two,three")
      conf.set("seq", "1.1,2.2,3.3")
      conf.set("vector", "true,false")
      CaseClassConfigParser[ListSeqVectorTest](context, "") should be(
        ListSeqVectorTest(List("one", "two", "three"), Seq(1.1, 2.2, 3.3), Vector(true, false))
      )
    }

  }

  describe("Exception configurations") {

    it("parsing a missing configuration should throw an exception") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      intercept[NoSuchElementException] {
        CaseClassConfigParser[MissingArgTest](context, "")
      }
    }

    it("parsing a configuration with a wrong prefix should throw an exception") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("spark.missing", "value")
      // Check for false positive result
      CaseClassConfigParser[MissingArgTest](context, "spark.") should be(MissingArgTest("value"))
      intercept[NoSuchElementException] {
        CaseClassConfigParser[MissingArgTest](context, "")
      }
    }

    it("parsing a configuration with wrong type should throw a parsing error") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("intError", "wrong")
      conf.set("optionIntError", "wrong")
      conf.set("booleanError", "wrong")
      intercept[NumberFormatException] {
        CaseClassConfigParser[ParseErrorIntTest](context, "")
      }
      intercept[NumberFormatException] {
        CaseClassConfigParser[ParseErrorOptionIntTest](context, "")
      }
      intercept[IllegalArgumentException] {
        CaseClassConfigParser[ParseErrorBooleanTest](context, "")
      }
    }

    it("parsing a case class with an unsupported type should throw an exception") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("unknown", "wrong")
      intercept[UnsupportedOperationException] {
        CaseClassConfigParser[ParseUnknownTypeTest](context, "")
      }
    }

    it("parsing a case class nested in a class should throw an exception with a helpful error") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      intercept[UnsupportedOperationException] {
        CaseClassConfigParser[ParseNestedClassTest](context, "")
      }
    }

    it("parsing a case class with an unsupported collection type should throw an exception") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set("unsupported", "1,2")
      intercept[UnsupportedOperationException] {
        CaseClassConfigParser[ParseUnsupportedCollectionTest](context, "")
      }
    }

  }

  describe("Parameter Providers configurations") {

    it("get parameters from properties file when it is defined") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set(CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES, "com.coxautodata.waimak.configuration.TestPropertyProvider")
      TestPropertyProvider.props.setProperty("test.string", "test1")
      TestPropertyProvider.getPropertyProvider(context).get("test.string") should be(Some("test1"))
      CaseClassConfigParser[PropertiesTest](context, "test.") should be(PropertiesTest("test1"))
    }

    it("get parameters from properties file when it is defined but the property doesn't exist") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set(CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES, "com.coxautodata.waimak.configuration.TestPropertyProvider")
      TestPropertyProvider.props.clear()
      TestPropertyProvider.getPropertyProvider(context).get("test.string") should be(None)
      intercept[NoSuchElementException] {
        CaseClassConfigParser[PropertiesTest](context, "test.")
      }.getMessage should be("No SparkConf configuration value, no value in any PropertyProviders or default value found for parameter test.string")
    }

    it("define a property provider module that doesn't exist") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set(CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES, "com.coxautodata.waimak.configuration.MissingModule")
      intercept[ScalaReflectionException] {
        CaseClassConfigParser[PropertiesTest](context, "test.")
      }.getMessage should be("object com.coxautodata.waimak.configuration.MissingModule not found.")
    }

    it("respect the order the property providers were given when resolving values") {

      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set(CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES, "com.coxautodata.waimak.configuration.TestPropertyProvider,com.coxautodata.waimak.configuration.TestPropertyProvider2")

      intercept[NoSuchElementException] {
        CaseClassConfigParser[PropertiesTest](context, "test.")
      }

      TestPropertyProvider2.props.setProperty("test.string", "2")
      CaseClassConfigParser[PropertiesTest](context, "test.") should be(PropertiesTest("2"))

      TestPropertyProvider.props.setProperty("test.string", "1")
      CaseClassConfigParser[PropertiesTest](context, "test.") should be(PropertiesTest("1"))

      conf.set("test.string", "0")
      CaseClassConfigParser[PropertiesTest](context, "test.") should be(PropertiesTest("0"))
    }

  }

  case class ParseNestedClassTest(string: String = "")

}

object TestPropertyProvider extends PropertyProviderBuilder {
  val props = new Properties()

  override def getPropertyProvider(conf: SparkFlowContext): PropertyProvider = new JavaPropertiesPropertyProvider(props)
}

object TestPropertyProvider2 extends PropertyProviderBuilder {
  val props = new Properties()

  override def getPropertyProvider(conf: SparkFlowContext): PropertyProvider = new JavaPropertiesPropertyProvider(props)
}