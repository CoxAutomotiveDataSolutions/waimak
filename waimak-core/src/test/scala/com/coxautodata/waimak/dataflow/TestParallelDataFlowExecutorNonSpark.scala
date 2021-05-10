package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.dataflow.DFExecutorPriorityStrategies._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class TestParallelDataFlowExecutorNonSpark extends AnyFunSpec with Matchers {

  val exceptionAction = new DataFlowAction {
    override val inputLabels: List[String] = List.empty
    override val outputLabels: List[String] = List.empty

    override def performAction[C <: FlowContext](inputs: DataFlowEntities, flowContext: C): Try[ActionResult] = Try {
      /**
        * Any Fatal exception as defined in [[scala.util.control.NonFatal]]
        */
      throw new NoClassDefFoundError("Test Exception")
    }
  }

  val emptyFlow = MockDataFlow.empty

  val executor = new ParallelDataFlowExecutor(ParallelActionScheduler(), NoReportingFlowReporter(), defaultPriorityStrategy)

  describe("execute") {

    it("non-recoverable exception in action") {

      val res = intercept[DataFlowException] {
        executor.execute(emptyFlow.addAction(exceptionAction))
      }
      res.cause shouldBe a[DataFlowException]

      res.cause.asInstanceOf[DataFlowException].cause shouldBe a[NoClassDefFoundError]
    }

  }
}
