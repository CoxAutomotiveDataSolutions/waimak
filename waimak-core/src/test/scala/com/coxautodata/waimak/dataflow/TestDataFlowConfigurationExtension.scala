package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.dataflow.spark.{SparkDataFlow, SparkSpec}

class TestDataFlowConfigurationExtension extends SparkSpec {
  override val appName: String = "TestDataFlowConfigurationExtension"

  describe("MockDataFlow") {
    it("No extensions enabled") {
      MockDataFlow.empty.getEnabledConfigurationExtensions should be(Seq.empty)
    }

    it("mockdataflow extension enabled") {
      val flowContext: EmptyFlowContext = new EmptyFlowContext
      flowContext.conf.setProperty("spark.waimak.dataflow.extensions", "mockdataflow")
      val res = MockDataFlow.empty.copy(flowContext = flowContext).getEnabledConfigurationExtensions
      res.length should be(1)
      res.head shouldBe a[TestMockDataFlowConfigurationExtension]
    }

    it("mockdataflow and sparkdataflow extensions enabled, only mockdataflow supported") {
      val flowContext: EmptyFlowContext = new EmptyFlowContext
      flowContext.conf.setProperty("spark.waimak.dataflow.extensions", "mockdataflow,sparkdataflow")
      intercept[DataFlowException] {
        MockDataFlow.empty.copy(flowContext = flowContext).getEnabledConfigurationExtensions
      }.text should be("The following extensions could not be found: [sparkdataflow]")
    }
  }

  describe("SparkDataFlow") {
    it("No extensions enabled") {
      SparkDataFlow.empty(sparkSession).getEnabledConfigurationExtensions should be(Seq.empty)
    }

    it("sparkdataflow extension enabled") {
      val spark = sparkSession
      spark.conf.set("spark.waimak.dataflow.extensions", "sparkdataflow")
      val res = SparkDataFlow.empty(spark).getEnabledConfigurationExtensions
      res.length should be(1)
      res.head shouldBe a[TestSparkDataFlowConfigurationExtension]
    }

    it("sparkdataflow and mockdataflow extensions enabled, only sparkdataflow supported") {
      val spark = sparkSession
      spark.conf.set("spark.waimak.dataflow.extensions", "sparkdataflow,mockdataflow")
      intercept[DataFlowException] {
        SparkDataFlow.empty(spark).getEnabledConfigurationExtensions
      }.text should be("The following extensions could not be found: [mockdataflow]")
    }
  }
}

class TestMockDataFlowConfigurationExtension extends DataFlowConfigurationExtension[MockDataFlow] {
  override def extensionKey: String = "mockdataflow"

  override def preExecutionManipulation(flow: MockDataFlow): MockDataFlow = flow
}

class TestSparkDataFlowConfigurationExtension extends DataFlowConfigurationExtension[SparkDataFlow] {
  override def extensionKey: String = "sparkdataflow"

  override def preExecutionManipulation(flow: SparkDataFlow): SparkDataFlow = flow
}