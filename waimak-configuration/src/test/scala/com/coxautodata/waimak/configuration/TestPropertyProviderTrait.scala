package com.coxautodata.waimak.configuration

import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.TimeoutException

class TestPropertyProviderTrait extends FunSpec with Matchers {

  describe("getWithTimeout") {

    it("should retry and succeed the third time") {
      new TestPropertyProviderInstance(List(new RuntimeException, new RuntimeException))
        .getWithRetry("", 200, 3) should be(Some("no timeout"))
    }

    it("should throw an exception") {
      intercept[RuntimeException] {
        new TestPropertyProviderInstance(List(new RuntimeException))
          .getWithRetry("", 200, 0)
      }
    }

  }

}

class TestPropertyProviderInstance(private var failures: List[Throwable]) extends PropertyProvider {

  override def get(key: String): Option[String] = synchronized {
    failures match {
      case Nil => Some("no timeout")
      case h :: t =>
        failures = t
        throw h
    }
  }
}
