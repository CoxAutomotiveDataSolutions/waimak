package com.coxautodata.waimak.dataflow

import org.scalatest.{FunSpec, Matchers}

class TestDataFlowAction extends FunSpec with Matchers {

  val func2 = () => List(Some("v1"), Some("v2"))


  describe("create") {

    it("check guid") {
      val res = new TestPresetAction(List("i1", "i2"), List("o1", "o2"),func2)
      res.guid.length should be(36)
    }

    it("check logLabel") {
      val res = new TestPresetAction(List("i1", "i2"), List("o1", "o2"),func2)
      res.logLabel should be(s"${res.guid} Inputs: [i1,i2] Outputs: [o1,o2]")
    }
  }

  describe("flow states") {

    val inputs = DataFlowEntities(Map("i1" -> Some("v1"), "i2" -> Some("v2"), "i3" -> None, "i4" -> None))

    describe("ready to run") {

      it("no input labels, requires all inputs") {
        val action = new TestPresetAction(List.empty, List("o1", "o2"), func2)
        val state = action.flowState(inputs)
        state should be(ReadyToRun(Seq.empty))
      }

      it("no input labels, does not require all inputs") {
        val action = new TestPresetAction(List.empty, List("o1", "o2"), func2, false)
        val state = action.flowState(inputs)
        state should be(ReadyToRun(Seq.empty))
      }

      it("single input in db and in action") {
        val action = new TestPresetAction(List("i1"), List("o1", "o2"), func2)
        val state = action.flowState(inputs)
        state should be(ReadyToRun(Seq("i1")))
      }

      it("multiple inputs, requires all") {
        val action = new TestPresetAction(List("i1", "i2"), List("o1", "o2"), func2)
        val state = action.flowState(inputs)
        state should be(ReadyToRun(Seq("i1", "i2")))
      }

      it("multiple inputs, does not require all") {
        val action = new TestPresetAction(List("i1", "i2"), List("o1", "o2"), func2, false)
        val state = action.flowState(inputs)
        state should be(ReadyToRun(Seq("i1", "i2")))
      }

      it("one input, does not require all") {
        val action = new TestPresetAction(List("i3"), List("o1", "o2"), func2, false)
        val state = action.flowState(inputs)
        state should be(ReadyToRun(Seq("i3")))
      }

      it("multiple defined inputs, one empty input, does not require all") {
        val action = new TestPresetAction(List("i1", "i2", "i3"), List("o1", "o2"), func2, false)
        val state = action.flowState(inputs)
        state should be(ReadyToRun(Seq("i1", "i2", "i3")))
      }

      it("multiple defined inputs, 2 empty inputs, does not require all") {
        val action = new TestPresetAction(List("i1", "i2", "i3", "i4"), List("o1", "o2"), func2, false)
        val state = action.flowState(inputs)
        state should be(ReadyToRun(Seq("i1", "i2", "i3", "i4")))
      }

    }

    describe("requires input") {

      it("one input, requires all") {
        val action = new TestPresetAction(List("m1"), List("o1", "o2"), func2)
        val state = action.flowState(inputs)
        state should be(RequiresInput(Seq.empty, Seq("m1")))
      }

      it("one input, does not require all") {
        val action = new TestPresetAction(List("m1"), List("o1", "o2"), func2, false)
        val state = action.flowState(inputs)
        state should be(RequiresInput(Seq.empty, Seq("m1")))
      }

      it("one present and one not present input, requires all") {
        val action = new TestPresetAction(List("i1", "m1"), List("o1", "o2"), func2)
        val state = action.flowState(inputs)
        state should be(RequiresInput(Seq("i1"), Seq("m1")))
      }

      it("one present and one not present input, does not require all") {
        val action = new TestPresetAction(List("i1", "m1"), List("o1", "o2"), func2, false)
        val state = action.flowState(inputs)
        state should be(RequiresInput(Seq("i1"), Seq("m1")))
      }

      it("present is empty and one not present input, does not require all") {
        val action = new TestPresetAction(List("i3", "m1"), List("o1", "o2"), func2, false)
        val state = action.flowState(inputs)
        state should be(RequiresInput(Seq("i3"), Seq("m1")))
      }

      it("present is empty and one not present input, requires all") {
        val action = new TestPresetAction(List("i3", "m1"), List("o1", "o2"), func2)
        val state = action.flowState(inputs)
        state should be(RequiresInput(Seq("i3"), Seq("m1")))
      }

      it("one present is not empty, another present is empty and one not present input, does not require all") {
        val action = new TestPresetAction(List("i1", "i3", "m1"), List("o1", "o2"), func2, false)
        val state = action.flowState(inputs)
        state should be(RequiresInput(Seq("i1", "i3"), Seq("m1")))
      }

      it("all multiple require input, requires all") {
        val action = new TestPresetAction(List("m1", "m3"), List("o1", "o2"), func2)
        val state = action.flowState(inputs)
        state should be(RequiresInput(Seq.empty, Seq("m1", "m3")))
      }

      it("all multiple require input, does not require all") {
        val action = new TestPresetAction(List("m1", "m3"), List("o1", "o2"), func2, false)
        val state = action.flowState(inputs)
        state should be(RequiresInput(Seq.empty, Seq("m1", "m3")))
      }

    }

    describe("expected input is empty") {

      it("one input, one empty") {
        val action = new TestPresetAction(List("i3"), List("o1", "o2"), func2)
        val state = action.flowState(inputs)
        state should be(ExpectedInputIsEmpty(Seq.empty, Seq("i3")))
      }

      it("2 inputs, 2 empty") {
        val action = new TestPresetAction(List("i3", "i4"), List("o1", "o2"), func2)
        val state = action.flowState(inputs)
        state should be(ExpectedInputIsEmpty(Seq.empty, Seq("i3", "i4")))
      }

      it("3 inputs, 2 empty") {
        val action = new TestPresetAction(List("i1", "i3", "i4"), List("o1", "o2"), func2)
        val state = action.flowState(inputs)
        state should be(ExpectedInputIsEmpty(Seq("i1"), Seq("i3", "i4")))
      }
    }

  }
}