package com.coxautodata.waimak.dataflow

import org.scalatest.{FunSpec, Matchers}

import scala.util.Success

/**
  * Created by Alexei Perelighin on 2018/08/13
  */
class TestParallelActionScheduler extends FunSpec with Matchers {

  val defaultPool = Set(DEFAULT_POOL_NAME)

  val func2 = () => {
    List(Some("v1"), Some("v2"))
  }

  val action1 = new TestPresetAction(List.empty, List("o1", "o2"),func2)

  val neverEndingAction = new TestPresetAction(List.empty, List("o1", "o2"), () => {Thread.sleep(86400000); List.empty } )

  val flowContext = new EmptyFlowContext

  val reporter = NoReportingFlowReporter()

  describe("With single thread") {

    val emptySchedulerOneThread = ParallelActionScheduler()

    val dummyBusySchedulerOneThread = {
      val poolDesc = emptySchedulerOneThread.pools(DEFAULT_POOL_NAME).addActionGUID(action1.guid)
      new ParallelActionScheduler(emptySchedulerOneThread.pools + (DEFAULT_POOL_NAME -> poolDesc), emptySchedulerOneThread.actionFinishedNotificationQueue)
    }

    describe("availableExecutionPool") {

      it("nothing is running") {
        emptySchedulerOneThread.availableExecutionPools should be (Some(Set(DEFAULT_POOL_NAME)))
      }

      it("action already running") {
        dummyBusySchedulerOneThread.availableExecutionPools should be (None)
      }

    }

    describe("dropRunning") {

      val toSchedule = Seq(action1, new TestPresetAction(List("o1", "o2"), List("o3", "o4"),func2))

      it("nothing is running") {
        emptySchedulerOneThread.dropRunning(defaultPool, toSchedule) should be(toSchedule)
      }

      it("action is running") {
        dummyBusySchedulerOneThread.dropRunning(defaultPool, toSchedule) should be(toSchedule.drop(1))
      }

      it("no action to schedule") {
        emptySchedulerOneThread.dropRunning(defaultPool, Seq.empty) should be(Seq.empty)
        dummyBusySchedulerOneThread.dropRunning(defaultPool, Seq.empty) should be(Seq.empty)
      }

    }

    describe("hasRunningActions") {

      it("nothing is running") {
        emptySchedulerOneThread.hasRunningActions should be(false)
      }

      it("an action is running") {
        dummyBusySchedulerOneThread.hasRunningActions should be(true)
      }

    }

    describe("waitToFinish") {

      /*
      Not sure if it is relevant, but can hang forever.
      it("nothing is running") {
        val error = emptySchedulerOneThread.waitToFinish()
        error.failed.get.getMessage should be("Error while waiting to finish")
      }
      */

      it("an input only one action is running") {
        val withAction = emptySchedulerOneThread.schedule(DEFAULT_POOL_NAME, action1, DataFlowEntities.empty, flowContext, reporter)
        val res = withAction.waitToFinish(flowContext, reporter)
        val scheduler = res._1.asInstanceOf[ParallelActionScheduler]
        scheduler.pools.get(DEFAULT_POOL_NAME).map(_.running.isEmpty) should be(Some(true))
        res._2 should be(Seq((action1, Success(List(Some("v1"), Some("v2"))))))
      }

      it("wait for failed action") {
        val failedAction = new TestPresetAction(List.empty, List("o1", "o2"), () => {throw new RuntimeException("ERROR 1") } )
        val withAction = emptySchedulerOneThread.schedule(DEFAULT_POOL_NAME, failedAction, DataFlowEntities.empty, flowContext, reporter)
        val res = withAction.waitToFinish(flowContext, reporter)

      }
    }

    describe("submitAction") {

      it("nothing is running before submitting") {
        val nextScheduler: ParallelActionScheduler = emptySchedulerOneThread.schedule(DEFAULT_POOL_NAME, action1, DataFlowEntities.empty, new EmptyFlowContext, reporter).asInstanceOf[ParallelActionScheduler]
        nextScheduler.pools.size should be(1)
        nextScheduler.pools.get(DEFAULT_POOL_NAME).map(_.running) should be(Some(Set(action1.guid)))
        nextScheduler.hasRunningActions should be(true)
        nextScheduler.availableExecutionPools should be(None)
      }

      //TODO: figure out a way to test when no slots are available in the Executor

    }

  }
}
