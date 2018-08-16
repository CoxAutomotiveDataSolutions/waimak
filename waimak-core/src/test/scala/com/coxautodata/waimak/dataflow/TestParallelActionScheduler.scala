package com.coxautodata.waimak.dataflow

import org.scalatest.{FunSpec, Matchers}

import scala.util.Success

/**
  * Created by Alexei Perelighin on 2018/08/13
  */
class TestParallelActionScheduler extends FunSpec with Matchers {

  val func2 = () => {
    List(Some("v1"), Some("v2"))
  }

  val action1 = new TestPresetAction(List.empty, List("o1", "o2"),func2)

  val neverEndingAction = new TestPresetAction(List.empty, List("o1", "o2"), () => {Thread.sleep(86400000); List.empty } )

  describe("With single thread") {

    val emptySchedulerOneThread = ParallelActionScheduler[EmptyFlowContext]()

    val dummyBusySchedulerOneThread = {
      val poolDesc = emptySchedulerOneThread.pools(DEFAULT_POOL_NAME).addActionGUID(action1.guid)
      new ParallelActionScheduler(emptySchedulerOneThread.pools + (DEFAULT_POOL_NAME -> poolDesc), emptySchedulerOneThread.actionFinishedNotificationQueue)
    }

    describe("availableExecutionPool") {

      it("nothing is running") {
        emptySchedulerOneThread.availableExecutionPool() should be (Some(DEFAULT_POOL_NAME))
      }

      it("action already running") {
        dummyBusySchedulerOneThread.availableExecutionPool() should be (None)
      }

    }

    describe("dropRunning") {

      val toSchedule = Seq(action1, new TestPresetAction(List("o1", "o2"), List("o3", "o4"),func2))

      it("nothing is running") {
        emptySchedulerOneThread.dropRunning(DEFAULT_POOL_NAME, toSchedule) should be(toSchedule)
      }

      it("action is running") {
        dummyBusySchedulerOneThread.dropRunning(DEFAULT_POOL_NAME, toSchedule) should be(toSchedule.drop(1))
      }

      it("no action to schedule") {
        emptySchedulerOneThread.dropRunning(DEFAULT_POOL_NAME, Seq.empty) should be(Seq.empty)
        dummyBusySchedulerOneThread.dropRunning(DEFAULT_POOL_NAME, Seq.empty) should be(Seq.empty)
      }

    }

    describe("hasRunningActions") {

      it("nothing is running") {
        emptySchedulerOneThread.hasRunningActions() should be(false)
      }

      it("an action is running") {
        dummyBusySchedulerOneThread.hasRunningActions() should be(true)
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
        val withAction = emptySchedulerOneThread.submitAction(DEFAULT_POOL_NAME, action1, DataFlowEntities.empty, new EmptyFlowContext)
        val res = withAction.waitToFinish()
        val scheduler = res.get._1.asInstanceOf[ParallelActionScheduler[EmptyFlowContext]]
        scheduler.pools.get(DEFAULT_POOL_NAME).map(_.running.isEmpty) should be(Some(true))
        res.get._2 should be(Seq((action1, Success(List(Some("v1"), Some("v2"))))))
      }

      it("wait for failed action") {
        val failedAction = new TestPresetAction(List.empty, List("o1", "o2"), () => {throw new RuntimeException("ERROR 1") } )
        val withAction = emptySchedulerOneThread.submitAction(DEFAULT_POOL_NAME, failedAction, DataFlowEntities.empty, new EmptyFlowContext)
        val res = withAction.waitToFinish()

      }
    }

    describe("submitAction") {

      it("nothing is running") {
        val nextScheduler: ParallelActionScheduler[EmptyFlowContext] = emptySchedulerOneThread.submitAction(DEFAULT_POOL_NAME, action1, DataFlowEntities.empty, new EmptyFlowContext).asInstanceOf[ParallelActionScheduler[EmptyFlowContext]]
        nextScheduler.pools.size should be(1)
        nextScheduler.pools.get(DEFAULT_POOL_NAME).map(_.running) should be(Some(Set(action1.guid)))
        nextScheduler.hasRunningActions() should be(true)
        nextScheduler.availableExecutionPool() should be(None)
      }

//      it("an input only one action is running") {
//        val res = schedulerWithAction.waitToFinish()
//        res.get._1.asInstanceOf[SequentialScheduler[EmptyFlowContext]].toRun should be(None)
//        res.get._2 should be(Seq((action1, Success(List(Some("v1"), Some("v2"))))))
//      }

    }

  }
}
