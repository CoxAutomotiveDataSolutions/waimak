package com.coxautodata.waimak.configuration

import java.util.Properties
import com.coxautodata.waimak.configuration.CaseClassConfigParser.CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES
import com.coxautodata.waimak.configuration.DatabricksSecretsPropertyProviderBuilder._
import com.coxautodata.waimak.dataflow.spark.{SparkFlowContext, SparkSpec}
import com.databricks.dbutils_v1.{DatabricksCredentialUtils, LibraryUtils}
import org.apache.spark.sql.RuntimeConfig

import java.util

class TestDatabricksSecretsPropertyProviderBuilder extends SparkSpec {

  override val appName: String = "TestDatabricksSecretsPropertyProviderBuilder"

  describe("getPropertyProvider") {

    it("should find a parameter from a provided secret scope") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set(CONFIG_DATABRICKS_SECRET_SCOPES, "scope1")

      val props = new Properties()
      props.setProperty("key", "value")
      com.databricks.dbutils_v1.DBUtilsHolder.dbutils0.set(new TestDBUtilsV1Secrets(Map("scope1" -> props)))

      val propProv = DatabricksSecretsPropertyProviderBuilder.getPropertyProvider(context)

      propProv.get("key") should be(Some("value"))
      propProv.get("missing") should be(None)
    }

    it("should find a parameter by checking available scopes") {
      val context = SparkFlowContext(sparkSession)
      context.getOption(CONFIG_DATABRICKS_SECRET_SCOPES) should be(None)

      val props = new Properties()
      props.setProperty("key", "value")
      com.databricks.dbutils_v1.DBUtilsHolder.dbutils0.set(new TestDBUtilsV1Secrets(Map("scope1" -> props)))

      val propProv = DatabricksSecretsPropertyProviderBuilder.getPropertyProvider(context)

      propProv.get("key") should be(Some("value"))
      propProv.get("missing") should be(None)
    }

    it("should check multiple scopes in order they're given") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set(CONFIG_DATABRICKS_SECRET_SCOPES, "scope1,scope2")

      val props1 = new Properties()
      val props2 = new Properties()

      com.databricks.dbutils_v1.DBUtilsHolder.dbutils0.set(new TestDBUtilsV1Secrets(Map("scope1" -> props1, "scope2" -> props2)))

      val propProv = DatabricksSecretsPropertyProviderBuilder.getPropertyProvider(context)

      propProv.get("key") should be(None)

      props2.setProperty("key", "2")
      propProv.get("key") should be(Some("2"))

      props1.setProperty("key", "1")
      propProv.get("key") should be(Some("1"))
    }

    it("should ignore a scope if it is not in the list provided") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set(CONFIG_DATABRICKS_SECRET_SCOPES, "scope1")

      val props1 = new Properties()
      val props2 = new Properties()

      com.databricks.dbutils_v1.DBUtilsHolder.dbutils0.set(new TestDBUtilsV1Secrets(Map("scope1" -> props1, "scope2" -> props2)))

      val propProv = DatabricksSecretsPropertyProviderBuilder.getPropertyProvider(context)

      propProv.get("key") should be(None)

      props2.setProperty("key", "2")
      propProv.get("key") should be(None)

      props1.setProperty("key", "1")
      propProv.get("key") should be(Some("1"))
    }

    it("should transform the key if it contains special characters") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf

      val props1 = new Properties()

      com.databricks.dbutils_v1.DBUtilsHolder.dbutils0.set(new TestDBUtilsV1Secrets(Map("scope1" -> props1)))

      props1.setProperty("test-test--test--test", "2")
      props1.setProperty("test.test..test!.test", "1")

      val propProv = DatabricksSecretsPropertyProviderBuilder.getPropertyProvider(context)
      propProv.get("test.test..test!.test") should be(Some("2"))

      conf.set(CONFIG_DATABRICKS_REPLACE_SPECIAL_CHARACTERS_IN_KEY, false)
      propProv.get("test.test..test!.test") should be(Some("1"))
    }
  }

  describe("CaseClassConfigParser") {

    it("should run a case class parse with the databricks configuration") {
      val context = SparkFlowContext(sparkSession)
      val conf: RuntimeConfig = sparkSession.conf
      conf.set(CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES, "com.coxautodata.waimak.configuration.DatabricksSecretsPropertyProviderBuilder")
      val props = new Properties()
      props.setProperty("key", "value")
      com.databricks.dbutils_v1.DBUtilsHolder.dbutils0.set(new TestDBUtilsV1Secrets(Map("scope1" -> props)))
      CaseClassConfigParser[DatabricksTest](context, "") should be(DatabricksTest("value"))
    }

  }

}

class TestDBUtilsV1Secrets(properties: Map[String, Properties]) extends com.databricks.dbutils_v1.DBUtilsV1 {
  override val widgets: com.databricks.dbutils_v1.WidgetsUtils = null
  override val meta: com.databricks.dbutils_v1.MetaUtils = null
  override val fs: com.databricks.dbutils_v1.DbfsUtils = null
  override val notebook: com.databricks.dbutils_v1.NotebookUtils = null
  override val secrets: com.databricks.dbutils_v1.SecretUtils = new com.databricks.dbutils_v1.SecretUtils {
    override def get(scope: String, key: String): String = {
      properties
        .get(scope)
        .flatMap(p => Option(p.getProperty(key)))
        .getOrElse(throw new IllegalArgumentException)
    }

    override def getBytes(scope: String, key: String): Array[Byte] = ???

    override def list(scope: String): Seq[com.databricks.dbutils_v1.SecretMetadata] = ???

    override def listScopes(): Seq[com.databricks.dbutils_v1.SecretScope] = properties.keys.map(com.databricks.dbutils_v1.SecretScope).toSeq

    override def help(): Unit = ???

    override def help(moduleOrMethod: String): Unit = ???
  }
  override val preview: com.databricks.dbutils_v1.Preview = null

  override def help(): Unit = ???

  override def help(moduleOrMethod: String): Unit = ???

  override val library: LibraryUtils = new LibraryUtils {
    override def install(path: String): Boolean = ???

    override def restartPython(): Unit = ???

    override def installPyPI(pypiPackage: String, version: String, repo: String, extras: String): Boolean = ???

    override def updateCondaEnv(envYmlContent: String): Boolean = ???

    override def list(): util.List[String] = ???

    override def help(): Unit = ???

    override def help(moduleOrMethod: String): Unit = ???
  }

  override val credentials: DatabricksCredentialUtils = new DatabricksCredentialUtils {
    override def assumeRole(role: String): Boolean = ???

    override def showCurrentRole(): util.List[String] = ???

    override def showRoles(): util.List[String] = ???

    override def help(): Unit = ???

    override def help(moduleOrMethod: String): Unit = ???
  }
}

case class DatabricksTest(key: String)