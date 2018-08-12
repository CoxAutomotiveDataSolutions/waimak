package com.coxautodata.waimak.dataflow

import org.scalatest.{FunSpec, Matchers}

/**
  * Created by Alexei Perelighin on 2018/07/19
  */
trait TestActionScheduler extends FunSpec with Matchers {

  def reusableTest(word: String) = {

    describe("Test 1 " + word) {

      it("it 1") {
        println("DDDDDDD " + word)
//        word should be("A")
      }

      it("it 2") {
        println("CCCCCC " + word)
      }

    }

  }

  def defaultTests[C](schedulerType: String, scheduler: ActionScheduler[C]): Unit = {

    describe("ActionScheduler of type " + schedulerType + " with one default pool") {

      it("availableExecutionPool") {

      }
    }
  }

}

class ATestActionScheduler extends TestActionScheduler {

  it should behave like reusableTest("A")

  it should behave like reusableTest("b")

}