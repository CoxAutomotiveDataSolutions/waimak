package com.coxautodata.waimak.dataflow

import org.scalatest.{FunSpec, Matchers}

import scala.util.Success

/**
  * Created by Alexei Perelighin on 2018/07/30
  */
class TestSequentialScheduler extends FunSpec with Matchers {

  val defaultPool = Set(DEFAULT_POOL_NAME)

  val func2 = () => List(Some("v1"), Some("v2"))

  val action1 = new TestPresetAction(List.empty, List("o1", "o2"),func2)

  val emptyScheduler = new SequentialScheduler(None)

  val schedulerWithAction = new SequentialScheduler(Some((action1, DataFlowEntities.empty, new EmptyFlowContext)))

  val flowContext = new EmptyFlowContext

  val reporter = NoReportingFlowReporter()

  describe("SequentialScheduler.availableExecutionPool") {

    it("nothing is running") {
      emptyScheduler.availableExecutionPools should be (Some(Set(DEFAULT_POOL_NAME)))
    }

    it("action already running") {
      schedulerWithAction.availableExecutionPools should be (None)
    }

  }

  describe("SequentialScheduler.dropRunning") {

    val toSchedule = Seq(action1, new TestPresetAction(List("o1", "o2"), List("o3", "o4"),func2))

    it("nothing is running") {
      emptyScheduler.dropRunning(defaultPool, toSchedule) should be(toSchedule)
    }

    it("action is running") {
      schedulerWithAction.dropRunning(defaultPool, toSchedule) should be(toSchedule.drop(1))
    }

    it("no action to schedule") {
      emptyScheduler.dropRunning(defaultPool, Seq.empty) should be(Seq.empty)
      schedulerWithAction.dropRunning(defaultPool, Seq.empty) should be(Seq.empty)
    }

  }

  describe("SequentialScheduler.hasRunningActions") {

    it("nothing is running") {
      emptyScheduler.hasRunningActions should be(false)
    }

    it("an action is running") {
      schedulerWithAction.hasRunningActions should be(true)
    }

  }

  describe("SequentialScheduler.waitToFinish") {

    it("nothing is running") {
      val error = intercept[RuntimeException](emptyScheduler.waitToFinish(flowContext, reporter))
      error.getMessage should be("Called waitToFinish when there is nothing to run")
    }

    it("an input only one action is running") {
      val res = schedulerWithAction.waitToFinish(flowContext, reporter)
      res._1.asInstanceOf[SequentialScheduler].toRun should be(None)
      res._2 should be(Seq((action1, Success(List(Some("v1"), Some("v2"))))))
    }

  }

  describe("SequentialScheduler.submitAction") {

    it("nothing is running") {
      val nextScheduler: SequentialScheduler = emptyScheduler.schedule(DEFAULT_POOL_NAME, action1, DataFlowEntities.empty, flowContext, reporter).asInstanceOf[SequentialScheduler]
      nextScheduler.toRun.isDefined should be(true)
      nextScheduler.toRun.get._1 should be(action1)
    }

    it("an input only one action is running") {
      val res = schedulerWithAction.waitToFinish(flowContext, reporter)
      res._1.asInstanceOf[SequentialScheduler].toRun should be(None)
      res._2 should be(Seq((action1, Success(List(Some("v1"), Some("v2"))))))
    }

  }
}
