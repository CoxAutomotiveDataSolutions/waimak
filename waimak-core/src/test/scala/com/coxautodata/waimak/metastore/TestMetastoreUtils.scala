package com.coxautodata.waimak.metastore

import java.util.Properties

import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

class TestMetastoreUtils extends SparkAndTmpDirSpec {
  override val appName: String = "Metastore Tests"

  describe("JDBCConnector") {
    it("Should get jdbc properties from jceks file and combine them with existing properties") {

      // Create local jceks file and put entries in
      val jceksFile = s"jceks://file$testingBaseDirName/creds.jceks"
      val entries = Map("user" -> "ringo", "password" -> "starr")
      createJceksWithEntries(jceksFile, entries, sparkSession.sparkContext.hadoopConfiguration)

      val jdbcMapping = Map("user" -> "jdbc.user", "password" -> "jdbc.password")
      val properties = new Properties()
      properties.setProperty("jdbc.timeout", "1")

      val jdbc = TestJDBCConnector(sparkSession, properties, jdbcMapping)

      jdbc.getAllProperties.toMap should contain theSameElementsAs Map("jdbc.user" -> "ringo", "jdbc.password" -> "starr", "jdbc.timeout" -> "1")

      // Test immutability
      jdbc.properties.toMap should contain theSameElementsAs Map("jdbc.timeout" -> "1")

    }

    it("Should not get properties from the jceks path if none are given") {

      val properties = new Properties()
      properties.setProperty("jdbc.timeout", "1")

      val jdbc = TestJDBCConnector(sparkSession, properties)

      jdbc.getAllProperties.toMap should contain theSameElementsAs Map("jdbc.timeout" -> "1")
      jdbc.properties.toMap should contain theSameElementsAs Map("jdbc.timeout" -> "1")

    }

    it("Should throw an exception if no credential store could be found") {

      val properties = new Properties()
      properties.setProperty("jdbc.timeout", "1")
      val jdbcMapping = Map("user" -> "jdbc.user", "password" -> "jdbc.password")
      val jdbc = TestJDBCConnector(sparkSession, properties, jdbcMapping)

      val res = intercept[MetastoreUtilsException] {
        jdbc.getAllProperties.toMap
      }
      res.text should be("Could not read secure parameter [user] as no jceks file is set using [hadoop.security.credential.provider.path]")
    }

    it("Should throw an exception if keys could not be found in an existing credential store") {
      // Create local jceks file and put entries in
      val jceksFile = s"jceks://file$testingBaseDirName/creds.jceks"
      val entries = Map("user" -> "ringo")
      createJceksWithEntries(jceksFile, entries, sparkSession.sparkContext.hadoopConfiguration)

      val properties = new Properties()
      properties.setProperty("jdbc.timeout", "1")
      val jdbcMapping = Map("user" -> "jdbc.user", "password" -> "jdbc.password")
      val jdbc = TestJDBCConnector(sparkSession, properties, jdbcMapping)

      val res = intercept[MetastoreUtilsException] {
        jdbc.getAllProperties.toMap
      }
      res.text should be(s"Could not find secure parameter [password] in any locations at [$jceksFile]")
    }

    it("Should throw an exception an incorrect jceks file was given") {
      // Create local jceks file and put entries in
      val jceksFile = s"jceks://file$testingBaseDirName/creds.jceks"
      sparkSession.sparkContext.hadoopConfiguration.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceksFile)

      val properties = new Properties()
      properties.setProperty("jdbc.timeout", "1")
      val jdbcMapping = Map("user" -> "jdbc.user", "password" -> "jdbc.password")
      val jdbc = TestJDBCConnector(sparkSession, properties, jdbcMapping)

      val res = intercept[MetastoreUtilsException] {
        jdbc.getAllProperties.toMap
      }
      res.text should be(s"Could not find secure parameter [user] in any locations at [$jceksFile]")
    }
  }

  def createJceksWithEntries(jceksURL: String, entries: Map[String, String], conf: Configuration): Unit = {
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceksURL)
    val provider = CredentialProviderFactory.getProviders(conf).get(0)
    entries.foreach {
      case (k, v) => provider.createCredentialEntry(k, v.toCharArray)
    }
    provider.flush()
  }

}


case class TestJDBCConnector(sparkSession: SparkSession,
                             properties: Properties = new Properties(),
                             secureProperties: Map[String, String] = Map.empty) extends JDBCConnector {
  override def driverName: String = ???

  override def jdbcString: String = ???

  override def hadoopConfiguration: Configuration = sparkSession.sparkContext.hadoopConfiguration
}