package com.coxautodata.waimak.dataflow

import org.scalatest.{FunSpec, Matchers}

class TestDataFlowMetadataExtension extends FunSpec with Matchers {

  describe("Stabilisation of extension manipulations") {

    it("should fail after maximum iterations reached") {
      val extension = TestMetadataExtension(15)

      intercept[DataFlowException] {
        MockDataFlow
          .empty
          .updateMetadataExtension[TestMetadataExtension](TestMetadataExtensionIdentifier, _ => Some(extension))
          .prepareForExecution()
          .get
      }.text should be("Maximum number of iterations [10] reached before extension manipulations stabilised. " +
        "You can increase this limit using the flag [spark.waimak.dataflow.maxIterationsForExtensionManipulationsToStabalise].")
    }

    it("should not fail if under maximum iterations reached") {
      val extension = TestMetadataExtension(10)

      MockDataFlow
        .empty
        .updateMetadataExtension[TestMetadataExtension](TestMetadataExtensionIdentifier, _ => Some(extension))
        .prepareForExecution()
        .get

    }

    it("should not fail if maximum iterations increased") {
      val extension = TestMetadataExtension(15)

      val context = new EmptyFlowContext
      context.conf.setProperty("spark.waimak.dataflow.maxIterationsForExtensionManipulationsToStabalise", "15")

      MockDataFlow
        .empty
        .copy(flowContext = context)
        .updateMetadataExtension[TestMetadataExtension](TestMetadataExtensionIdentifier, _ => Some(extension))
        .prepareForExecution()
        .get

    }

  }

}

case class TestMetadataExtension(timeToStabilise: Int, count: Int = 0) extends DataFlowMetadataExtension[MockDataFlow] {

  override def preExecutionManipulation(flow: MockDataFlow): MockDataFlow = {
    if (count >= timeToStabilise) flow.updateMetadataExtension[TestMetadataExtension](identifier, _ => None)
    else flow.updateMetadataExtension[TestMetadataExtension](identifier, _ => Some(this.copy(count = count + 1)))
  }

  override def identifier: DataFlowMetadataExtensionIdentifier = TestMetadataExtensionIdentifier

}

case object TestMetadataExtensionIdentifier extends DataFlowMetadataExtensionIdentifier