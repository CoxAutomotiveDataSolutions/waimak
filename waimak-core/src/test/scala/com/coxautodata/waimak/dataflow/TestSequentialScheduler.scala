package com.coxautodata.waimak.dataflow

import org.scalatest.{FunSpec, Matchers}

import scala.util.Success

/**
  * Created by Alexei Perelighin on 2018/07/30
  */
class TestSequentialScheduler extends FunSpec with Matchers {

  val func2 = () => List(Some("v1"), Some("v2"))

  val action1 = new TestPresetAction(List.empty, List("o1", "o2"),func2)

  val emptyScheduler = new SequentialScheduler[EmptyFlowContext](None)

  val schedulerWithAction = new SequentialScheduler[EmptyFlowContext](Some((action1, DataFlowEntities.empty, new EmptyFlowContext)))

  describe("SequentialScheduler.availableExecutionPool") {

    it("nothing is running") {
      emptyScheduler.availableExecutionPool() should be (Some(DEFAULT_POOL_NAME))
    }

    it("action already running") {
      schedulerWithAction.availableExecutionPool() should be (None)
    }

  }

  describe("SequentialScheduler.dropRunning") {

    val toSchedule = Seq(action1, new TestPresetAction(List("o1", "o2"), List("o3", "o4"),func2))

    it("nothing is running") {
      emptyScheduler.dropRunning(DEFAULT_POOL_NAME, toSchedule) should be(toSchedule)
    }

    it("action is running") {
      schedulerWithAction.dropRunning(DEFAULT_POOL_NAME, toSchedule) should be(toSchedule.drop(1))
    }

    it("no action to schedule") {
      emptyScheduler.dropRunning(DEFAULT_POOL_NAME, Seq.empty) should be(Seq.empty)
      schedulerWithAction.dropRunning(DEFAULT_POOL_NAME, Seq.empty) should be(Seq.empty)
    }

  }

  describe("SequentialScheduler.hasRunningActions") {

    it("nothing is running") {
      emptyScheduler.hasRunningActions() should be(false)
    }

    it("an action is running") {
      schedulerWithAction.hasRunningActions() should be(true)
    }

  }

  describe("SequentialScheduler.waitToFinish") {

    it("nothing is running") {
      val error = emptyScheduler.waitToFinish()
      error.failed.get.getMessage should be("Error while waiting to finish")
    }

    it("an input only one action is running") {
      val res = schedulerWithAction.waitToFinish()
      res.get._1.asInstanceOf[SequentialScheduler[EmptyFlowContext]].toRun should be(None)
      res.get._2 should be(Seq((action1, Success(List(Some("v1"), Some("v2"))))))
    }

  }

  describe("SequentialScheduler.submitAction") {

    it("nothing is running") {
      val nextScheduler: SequentialScheduler[EmptyFlowContext] = emptyScheduler.submitAction(DEFAULT_POOL_NAME, action1, DataFlowEntities.empty, new EmptyFlowContext).asInstanceOf[SequentialScheduler[EmptyFlowContext]]
      nextScheduler.toRun.isDefined should be(true)
      nextScheduler.toRun.get._1 should be(action1)
    }

    it("an input only one action is running") {
      val res = schedulerWithAction.waitToFinish()
      res.get._1.asInstanceOf[SequentialScheduler[EmptyFlowContext]].toRun should be(None)
      res.get._2 should be(Seq((action1, Success(List(Some("v1"), Some("v2"))))))
    }

  }
}
