package com.coxautodata.waimak.dataflow

import org.scalatest.{FunSpec, Matchers}

class DataFlowConfigurationExtension extends FunSpec with Matchers{

  it("No extensions enabled"){
    MockDataFlow.empty.getEnabledConfigurationExtensions should be (Seq.empty)
  }

}
