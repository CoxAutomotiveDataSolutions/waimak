package com.coxautodata.waimak.configuration

import java.io.FileNotFoundException

import com.coxautodata.waimak.configuration.CaseClassConfigParser._
import com.coxautodata.waimak.dataflow.spark.{SparkAndTmpDirSpec, SparkFlowContext}
import org.apache.spark.sql.RuntimeConfig

class TestPropertiesFilePropertyProviderBuilder extends SparkAndTmpDirSpec {

  override val appName: String = "Test Secure Credentials"

  describe("getPropertyProvider") {

    it("should read a property with a unicode character from a valid file") {

      val path = this.getClass.getResource("/waimak.utils.securecredentials/test.properties").toURI.toString

      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set(CONFIG_PROPERTIES_FILE_URI, path)
      val prop = PropertiesFilePropertyProviderBuilder.getPropertyProvider(context)

      prop.get("key") should be(Some("specialValue£"))

    }

    it("should thrown an exception when the file does not exist") {

      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set(CONFIG_PROPERTIES_FILE_URI, "file:/junk")

      intercept[FileNotFoundException]{
        PropertiesFilePropertyProviderBuilder.getPropertyProvider(context)
      }

    }

    it("should thrown an exception when the configuration parameter has not been set") {

      val context = SparkFlowContext(sparkSession)

      intercept[NoSuchElementException]{
        PropertiesFilePropertyProviderBuilder.getPropertyProvider(context)
      }.getMessage should be ("Could not find value for property: spark.waimak.config.propertiesFileURI")

    }

  }

  describe("CaseClassConfigParser"){

    it("should take a configuration value from the properties file"){
      val path = this.getClass.getResource("/waimak.utils.securecredentials/test.properties").toURI.toString
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set(CONFIG_PROPERTIES_FILE_URI, path)
      conf.set(CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES, "com.coxautodata.waimak.configuration.PropertiesFilePropertyProviderBuilder")

      CaseClassConfigParser[TestConfiguration](context, "") should be (TestConfiguration("specialValue£"))
    }

  }

}

case class TestConfiguration(key: String)