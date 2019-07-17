package com.coxautodata.waimak.dataflow

import org.scalatest.{FunSpec, Matchers}

class TestDataFlowExtension extends FunSpec with Matchers {

  describe("Stabilisation of extension manipulations") {

    it("should fail after maximum iterations reached") {
      val extension = new TestExtension(15)

      intercept[DataFlowException] {
        MockDataFlow
          .empty
          .updateExtensionMetadata(extension, _ => extension.initialState)
          .prepareForExecution()
          .get
      }.text should be("Maximum number of iterations [10] reached before extension manipulations stabilised. " +
        "You can increase this limit using the flag [spark.waimak.dataflow.maxIterationsForExtensionManipulationsToStabalise].")
    }

    it("should not fail if under maximum iterations reached") {
      val extension = new TestExtension(10)

      MockDataFlow
        .empty
        .updateExtensionMetadata(extension, _ => extension.initialState)
        .prepareForExecution()
        .get

    }

    it("should not fail if maximum iterations increased") {
      val extension = new TestExtension(15)

      val context = new EmptyFlowContext
      context.conf.setProperty("spark.waimak.dataflow.maxIterationsForExtensionManipulationsToStabalise", "15")

      MockDataFlow
        .empty
        .copy(flowContext = context)
        .updateExtensionMetadata(extension, _ => extension.initialState)
        .prepareForExecution()
        .get

    }

  }

}

class TestExtension(val timeToStabilise: Int) extends DataFlowExtension[MockDataFlow] {

  var count: Int = 0

  override def initialState: DataFlowMetadataState = TestMetadataState

  override def preExecutionManipulation(flow: MockDataFlow, meta: DataFlowMetadataState): Option[MockDataFlow] = {
    if (count >= timeToStabilise) None
    else {
      count += 1
      Some(flow)
    }
  }
}

object TestMetadataState extends DataFlowMetadataState