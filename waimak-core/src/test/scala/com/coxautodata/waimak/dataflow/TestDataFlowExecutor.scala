package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.dataflow.DFExecutorPriorityStrategies.priorityStrategy
import com.coxautodata.waimak.dataflow.TestExecutor.TestScheduler
import com.coxautodata.waimak.dataflow.spark.SimpleAction
import org.scalatest.{FunSpec, Matchers}

import scala.collection.mutable.Queue
import scala.util.{Failure, Success, Try}

/**
  * Created by Vicky Avison on 13/12/18.
  */
class TestDataFlowExecutor extends FunSpec with Matchers {

  describe("waitForAnActionToFinish") {
    it("should allow running actions to complete before returning a failure") {
      val firstFailure = new RuntimeException("Error")
      val res = TestExecutor.waitForAnActionToFinish(MockDataFlow.empty, TestScheduler(true, Failure(firstFailure)), Nil)
      TestExecutor.numberOfCallsToWaitToFinish should be(4)
      TestExecutor.running should be(false)
      res.isFailure should be(true)
      res.failed.get shouldBe a[DataFlowException]
      res.failed.get.asInstanceOf[DataFlowException].cause should be (firstFailure)
    }
  }
}

object TestExecutor extends DataFlowExecutor {

  var running: Boolean = true

  var numberOfCallsToWaitToFinish: Int = 0

  var schedulers: Queue[ActionScheduler] = Queue(
    TestScheduler(true, Success(Nil))
    , TestScheduler(true, Failure(new RuntimeException("Another error")))
    , TestScheduler(true, Success(Nil))
    , TestScheduler(false, Success(Nil))
  )

  case class TestScheduler(hasRunningActions: Boolean, nextActionResult: Try[ActionResult]) extends ActionScheduler {

    override def availableExecutionPools(): Option[Set[String]] = ???

    override def dropRunning(poolNames: Set[String], from: Seq[DataFlowAction]): Seq[DataFlowAction] = ???

    override def waitToFinish(flowContext: FlowContext, flowReporter: FlowReporter): (ActionScheduler, Seq[(DataFlowAction, Try[ActionResult])]) = {
      val nextSched = schedulers.dequeue()
      numberOfCallsToWaitToFinish = numberOfCallsToWaitToFinish + 1
      running = nextSched.hasRunningActions
      (nextSched, Seq((new SimpleAction(Nil, Nil, _ => Nil), nextActionResult)))
    }

    override def schedule(poolName: String, action: DataFlowAction, entities: DataFlowEntities, flowContext: FlowContext, flowReporter: FlowReporter): ActionScheduler = ???

    override def shutDown(): Try[ActionScheduler] = ???
  }


  override def flowReporter: FlowReporter = NoReportingFlowReporter()

  override def priorityStrategy: priorityStrategy = ???

  override def initActionScheduler(): ActionScheduler = ???
}
