package com.coxautodata.waimak.configuration

import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.TimeoutException
import scala.util.Try

class TestPropertyProviderTrait extends FunSpec with Matchers {

  describe("getWithTimeout") {

    it("should retry and succeed the third time") {
      TestPropertyProviderInstance(List(1000, 1000))
        .getWithRetry("", 0, 3) should be(Some("no timeout"))
    }

    it("should retry and succeed with a shorter timeout") {
      TestPropertyProviderInstance(List(1000, 0))
        .getWithRetry("", 200, 3) should be(Some("after timeout"))
    }

    it("should throw an exception due to timeout") {
      intercept[TimeoutException] {
        TestPropertyProviderInstance(List(1000))
          .getWithRetry("", 200, 0)
      }
    }

  }

}

case class TestPropertyProviderInstance(failures: List[Long]) extends PropertyProvider {

  private val internalList = failures.toBuffer

  override def get(key: String): Option[String] =
    if (internalList.nonEmpty) {
      val timeout = internalList.remove(0)
      Thread.sleep(timeout)
      Some("after timeout")
    }
    else Some("no timeout")
}
