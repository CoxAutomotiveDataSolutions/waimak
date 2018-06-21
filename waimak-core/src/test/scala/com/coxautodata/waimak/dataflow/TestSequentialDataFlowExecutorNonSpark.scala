package com.coxautodata.waimak.dataflow

import org.scalatest.{FunSpec, Matchers}

import scala.collection.mutable

/**
  * Created by Alexei Perelighin on 2018/01/11.
  */
class TestSequentialDataFlowExecutorNonSpark extends FunSpec with Matchers {

  val func3: DataFlowEntities[String] => ActionResult[String] = ent => {
    ent.getAll
    List(Some("v1"), Some("v2"))
  }

  val func2: () => ActionResult[String] = () => List(Some("v1"), Some("v2"))

  val func1 = (w: Seq[Option[String]]) => () => w

  val emptyFlow = SimpleDataFlow.empty[String]

  val executor = new SequentialDataFlowExecutor[String, EmptyFlowContext](NoReportingFlowReporter.apply)

  describe("executeWave") {

    it("empty flow") {
      val res = executor.executeWave(emptyFlow)
      res._1 should be(Seq.empty)
      res._2.inputs should be(DataFlowEntities.empty)
      res._2.actions should be(Seq.empty)
    }

    describe("one wave, one action") {

      it("executed one action, no input label") {
        val action = new TestPresetAction(List.empty, List("t1", "t2"), func2)
        val flow = emptyFlow.addAction(action)

        val res = executor.executeWave(flow)
        res._1 should be(Seq(action))
        res._2.inputs should be(DataFlowEntities(Map("t1" -> Some("v1"), "t2" -> Some("v2"))))
        res._2.actions should be(Seq.empty)
      }

      it("executed one action, with input label") {
        val action = new TestPresetAction(List("i1"), List("t1", "t2"), func2)
        val flow = emptyFlow.addInput("i1", Some("vi")).addAction(action)

        val res = executor.executeWave(flow)
        res._1 should be(Seq(action))
        res._2.inputs should be(DataFlowEntities(Map("t1" -> Some("v1"), "t2" -> Some("v2"), "i1" -> Some("vi"))))
        res._2.actions should be(Seq.empty)
      }


      it("action does not have non empty input") {
        val action = new TestPresetAction(List("i1"), List("t1", "t2"), func2, true)
        val flow = emptyFlow.addInput("i1", None).addAction(action)

        val res = executor.executeWave(flow)
        res._1 should be(Seq.empty)
        res._2.inputs should be(DataFlowEntities(Map("i1" -> None)))
        res._2.actions should be(Seq(action))
      }

      it("action is run that does not require all inputs, an input is empty and the evaluation is forced") {
        val action = new TestInputAction(List("i1"), List("t1", "t2"), func3, false)
        val flow = emptyFlow.addInput("i1", None).addAction(action)

        val res = executor.executeWave(flow)
        res._1 should be(Seq(action))
        res._2.inputs should be(DataFlowEntities(Map("i1" -> None, "t1" -> Some("v1"), "t2" -> Some("v2"))))
        res._2.actions should be(Seq.empty)
      }
    }

    describe("one wave, multiple actions") {

      it("executed 2 actions, no input label") {
        val action_1 = new TestPresetAction(List.empty, List("t1", "t2"), func2)
        val action_2 = new TestPresetAction(List.empty, List("t3", "t4"), func2)
        val flow = emptyFlow.addAction(action_1).addAction(action_2)

        val res = executor.executeWave(flow)
        res._1 should be(Seq(action_1, action_2))
        res._2.inputs should be(DataFlowEntities(Map("t1" -> Some("v1"), "t2" -> Some("v2"), "t3" -> Some("v1"), "t4" -> Some("v2"))))
        res._2.actions should be(Seq.empty)
      }

      it("executed 1 action, one input label") {
        val action_1 = new TestPresetAction(List.empty, List("t1", "t2"), func2)
        val action_2 = new TestPresetAction(List("i1"), List("t3", "t4"), func2, false)
        val flow = emptyFlow.addInput("i1", None).addAction(action_1).addAction(action_2)

        val res = executor.executeWave(flow)
        res._1 should be(Seq(action_1, action_2))
        res._2.inputs should be(DataFlowEntities(Map("t1" -> Some("v1"), "t2" -> Some("v2"), "t3" -> Some("v1"), "t4" -> Some("v2"), "i1" -> None)))
        res._2.actions should be(Seq.empty)
      }
    }

    describe("2 waves, multiple actions") {

      it("executed 2 actions, no input label") {
        val action_1 = new TestPresetAction(List.empty, List("t1", "t2"), func2)
        val action_2 = new TestPresetAction(List.empty, List("t3", "t4"), func2)
        val action_3 = new TestPresetAction(List("t1", "t3"), List("t5", "t6"), func2)
        val action_4 = new TestPresetAction(List("t2", "t4"), List("t7", "t8"), func2)
        val flow = emptyFlow.addAction(action_1).addAction(action_2)
          .addAction(action_3).addAction(action_4)

        val res = executor.executeWave(flow)
        res._1 should be(Seq(action_1, action_2))
        res._2.inputs should be(DataFlowEntities(Map("t1" -> Some("v1"), "t2" -> Some("v2"), "t3" -> Some("v1"), "t4" -> Some("v2"))))
        res._2.actions should be(Seq(action_3, action_4))

        val res2 = executor.executeWave(res._2)

        res2._1 should be(Seq(action_3, action_4))
        res2._2.inputs should be(DataFlowEntities(Map("t1" -> Some("v1"), "t2" -> Some("v2"), "t3" -> Some("v1"), "t4" -> Some("v2")
          , "t5" -> Some("v1"), "t6" -> Some("v2"), "t7" -> Some("v1"), "t8" -> Some("v2"))))
        res2._2.actions should be(Seq.empty)
      }

    }

    describe("execute") {

      it("executed 2 waves, all executed") {
        val action_1 = new TestPresetAction(List.empty, List("t1", "t2"), func2)
        val action_2 = new TestPresetAction(List.empty, List("t3", "t4"), func2)
        val action_3 = new TestPresetAction(List("t1", "t3"), List("t5", "t6"), func2)
        val action_4 = new TestPresetAction(List("t2", "t4"), List("t7", "t8"), func2)
        val flow = emptyFlow.addAction(action_1).addAction(action_2)
          .addAction(action_3).addAction(action_4)

        val res = executor.execute(flow)

        res._1 should be(Seq(action_1, action_2, action_3, action_4))
        res._2.inputs should be(DataFlowEntities(Map("t1" -> Some("v1"), "t2" -> Some("v2"), "t3" -> Some("v1"), "t4" -> Some("v2")
          , "t5" -> Some("v1"), "t6" -> Some("v2"), "t7" -> Some("v1"), "t8" -> Some("v2"))))
        res._2.actions should be(Seq.empty)
      }

      it("executed 2 waves, one action in wave 1 returned empty") {
        val action_1 = new TestPresetAction(List.empty, List("t1", "t2"), func2)
        val action_2 = new TestPresetAction(List.empty, List("t3", "t4"), func1(Seq(Some("v3"), None)))
        val action_3 = new TestPresetAction(List("t1", "t2"), List("t5", "t6"), func2)
        val action_4 = new TestPresetAction(List("t2", "t4"), List("t7", "t8"), func2)
        val flow = emptyFlow.addAction(action_1).addAction(action_2)
          .addAction(action_3).addAction(action_4)

        val res = executor.execute(flow)

        res._1 should be(Seq(action_1, action_2, action_3))
        res._2.inputs should be(DataFlowEntities(Map("t1" -> Some("v1"), "t2" -> Some("v2"), "t3" -> Some("v3"), "t4" -> None
          , "t5" -> Some("v1"), "t6" -> Some("v2"))))
        res._2.actions should be(Seq(action_4))
      }
    }

    describe("flowReporter") {
      it("log executed actions in the reporter") {

        val action_1 = new TestPresetAction(List.empty, List("t1", "t2"), func2)
        val action_2 = new TestPresetAction(List.empty, List("t3", "t4"), func2)
        val flow = emptyFlow.addAction(action_1).addAction(action_2)

        val reporter = new TestReporter()
        val reportedExecutor = new SequentialDataFlowExecutor[String, EmptyFlowContext](reporter)

        val res = reportedExecutor.executeWave(flow)
        res._1 should be(Seq(action_1, action_2))
        res._2.inputs should be(DataFlowEntities(Map("t1" -> Some("v1"), "t2" -> Some("v2"), "t3" -> Some("v1"), "t4" -> Some("v2"))))
        res._2.actions should be(Seq.empty)

        reporter.reports.mkString(", ") should be(
          "Start: Action: TestPresetAction Inputs: [] Outputs: [t1,t2], " +
            "Finish: Action: TestPresetAction Inputs: [] Outputs: [t1,t2], " +
            "Start: Action: TestPresetAction Inputs: [] Outputs: [t3,t4], " +
            "Finish: Action: TestPresetAction Inputs: [] Outputs: [t3,t4]"
        )

      }
    }
  }
}

class TestReporter extends FlowReporter[String, EmptyFlowContext] {

  val reports: mutable.MutableList[String] = mutable.MutableList.empty[String]

  override def reportActionStarted(action: DataFlowAction[String, EmptyFlowContext], flowContext: EmptyFlowContext): Unit = reports += s"Start: ${action.description}"

  override def reportActionFinished(action: DataFlowAction[String, EmptyFlowContext], flowContext: EmptyFlowContext): Unit = reports += s"Finish: ${action.description}"
}
