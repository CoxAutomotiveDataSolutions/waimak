package com.coxautodata.waimak.configuration

import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec

class TestSecureCredentialUtils extends SparkAndTmpDirSpec {
  val basePath = "./target/test-classes/waimak.utils.securecredentials/"
  override val appName: String = "Test Secure Credentials"

  describe("getPropertiesFile") {

    it("should read a property with a unicode character") {

      val prop = SecureCredentialUtils.getPropertiesFile(s"${basePath}test.properties", sparkSession)

      prop.right.get.get("key") should be("specialValue£")

    }
  }

}