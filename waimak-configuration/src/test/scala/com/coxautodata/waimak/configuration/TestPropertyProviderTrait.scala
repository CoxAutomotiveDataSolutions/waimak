package com.coxautodata.waimak.configuration

import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.TimeoutException

class TestPropertyProviderTrait extends FunSpec with Matchers {

  describe("getWithTimeout") {

    it("should retry and succeed the third time") {
      new TestPropertyProviderInstance(List(1000, 1000))
        .getWithRetry("", 0, 3) should be(Some("no timeout"))
    }

    it("should retry and succeed with a shorter timeout") {
      new TestPropertyProviderInstance(List(1000, 0))
        .getWithRetry("", 200, 3) should be(Some("after timeout"))
    }

    it("should throw an exception due to timeout") {
      intercept[TimeoutException] {
        new TestPropertyProviderInstance(List(1000))
          .getWithRetry("", 200, 0)
      }
    }

  }

}

class TestPropertyProviderInstance(@volatile private var failures: List[Long]) extends PropertyProvider {

  override def get(key: String): Option[String] = failures match {
    case Nil => Some("no timeout")
    case h :: t =>
      Thread.sleep(h)
      failures = t
      Some("after timeout")
  }

}
