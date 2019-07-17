package com.coxautodata.waimak.dataflow

import java.util.UUID

import com.coxautodata.waimak.dataflow.DataFlow._
import org.scalatest.{FunSpec, Matchers}

import scala.util.{Success, Try}

class TestSparkDataFlow extends FunSpec with Matchers {

  val defaultPool = Set(DEFAULT_POOL_NAME)

  describe("Add actions success") {

    it("empty") {
      val emptyFlow = MockDataFlow.empty
      emptyFlow.inputs.size should be(0)
      emptyFlow.actions.size should be(0)
    }

    it("add 1 action, no action input") {
      val emptyFlow = MockDataFlow.empty
      val res = emptyFlow.addAction(new TestEmptyAction(List.empty, List("table_1")))
      res.inputs.size should be(0)
      res.actions.size should be(1)
    }

    it("add 1 action, empty input and output") {
      val emptyFlow = MockDataFlow.empty
      val res = emptyFlow.addAction(new TestEmptyAction(List.empty, List.empty))
      res.inputs.size should be(0)
      res.actions.size should be(1)
    }

    it("add 2 actions, no action input") {
      val emptyFlow = MockDataFlow.empty
      val res = emptyFlow
        .addAction(new TestEmptyAction(List.empty, List("table_1")))
        .addAction(new TestEmptyAction(List.empty, List("table_2")))
      res.inputs.size should be(0)
      res.actions.size should be(2)
    }

    it("add 2 actions, with chained action inputs") {
      val emptyFlow = MockDataFlow.empty
      val res = emptyFlow
        .addAction(new TestEmptyAction(List.empty, List("table_1")))
        .addAction(new TestEmptyAction(List("table_1"), List("table_2")))
      res.inputs.size should be(0)
      res.actions.size should be(2)
    }

    it("add 2 actions, with chained action inputs added in reverse") {
      val emptyFlow = MockDataFlow.empty
      val res = emptyFlow
        .addAction(new TestEmptyAction(List("table_1"), List("table_2")))
        .addAction(new TestEmptyAction(List.empty, List("table_1")))
        .prepareForExecution().get
      res.inputs.size should be(0)
      res.actions.size should be(2)
    }

    it("add 2 actions, with last empty output label") {
      val emptyFlow = MockDataFlow.empty
      val res = emptyFlow
        .addAction(new TestEmptyAction(List.empty, List("table_1")))
        .addAction(new TestEmptyAction(List("table_1"), List.empty)) // this happens when data is saved
      res.inputs.size should be(0)
      res.actions.size should be(2)
    }

    it("add 1 action, with existing empty input") {
      val emptyFlow = MockDataFlow.empty
      val res = emptyFlow
        .addInput("test_1", None)
        .addAction(new TestEmptyAction(List("test_1"), List.empty))
      res.inputs.size should be(1)
      res.actions.size should be(1)
    }

    it("add 1 action, with existing input") {
      val emptyFlow = MockDataFlow.empty
      val res = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addAction(new TestEmptyAction(List("test_1"), List.empty))
      res.inputs.size should be(1)
      res.actions.size should be(1)
    }
  }

  describe("Add actions fail") {

    it("add action, with output same name as existing input") {
      val emptyFlow = MockDataFlow.empty
      val res = intercept[DataFlowException] {
        emptyFlow
          .addInput("test_1", Some("value_1"))
          .addInput("test_3", Some("value_3"))
          .addAction(new TestEmptyAction(List("test_1"), List("test_3")))
      }
      res.text should be(s"Output label [test_3] is already in the inputs or is produced by another action.")
    }

    it("add action, with output same name as an existing output") {
      val emptyFlow = MockDataFlow.empty
      val res = intercept[DataFlowException] {
        emptyFlow
          .addInput("test_1", Some("value_1"))
          .addAction(new TestEmptyAction(List("test_1"), List("test_2")))
          .addAction(new TestEmptyAction(List("test_1"), List("test_3")))
          .addAction(new TestEmptyAction(List("test_2"), List("test_3")))
      }
      res.text should be(s"Output label [test_3] is already in the inputs or is produced by another action.")
    }
  }

  describe("Selection of runnable") {

    it("empty queued") {
      val emptyFlow = MockDataFlow.empty
      val res = emptyFlow.nextRunnable(defaultPool)
      res should be(Seq.empty)
    }

    it("empty input, no actions") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", None)
      val res = flow.nextRunnable(defaultPool)
      res should be(Seq.empty)
    }

    it("one input, no actions") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
      val res = flow.nextRunnable(defaultPool)
      res should be(Seq.empty)
    }

    it("one empty input, one action") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", None)
        .addAction(new TestEmptyAction(List("test_1"), List("test_2")))
      val res = flow.nextRunnable(defaultPool)
      res should be(Seq.empty)
    }

    it("2 inputs, one empty, action inputs from empty") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", None)
        .addInput("test_2", Some("value_2"))
        .addAction(new TestEmptyAction(List("test_1"), List("test_3")))
      val res = flow.nextRunnable(defaultPool)
      res should be(Seq.empty)
    }

    it("2 inputs, one empty, action inputs both") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", None)
        .addInput("test_2", Some("value_2"))
        .addAction(new TestEmptyAction(List("test_1", "test_2"), List("test_3")))
      val res = flow.nextRunnable(defaultPool)
      res should be(Seq.empty)
    }

    it("2 inputs, one empty, action inputs from non empty") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", None)
        .addInput("test_2", Some("value_2"))
        .addAction(new TestEmptyAction(List("test_2"), List("test_3")))
      val res = flow.nextRunnable(defaultPool)
      res.size should be(1)
      res(0).inputLabels should be(List("test_2"))
    }

    it("one non empty input, one action") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addAction(new TestEmptyAction(List("test_1"), List("test_2")))
      val res = flow.nextRunnable(defaultPool)
      res.size should be(1)
      res(0).inputLabels should be(List("test_1"))
    }

    it("multiple non-empty inputs, multiple actions, all ready") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addInput("test_2", Some("value_2"))
        .addInput("test_3", Some("value_3"))
        .addInput("test_4", Some("value_4"))
        .addAction(new TestEmptyAction(List("test_1"), List("test_21")))
        .addAction(new TestEmptyAction(List("test_1", "test_2"), List("test_22")))
        .addAction(new TestEmptyAction(List("test_3", "test_2"), List("test_23")))
        .addAction(new TestEmptyAction(List("test_1"), List("test_24")))

      val res = flow.nextRunnable(defaultPool)
      res.size should be(4)
      res(0).inputLabels should be(List("test_1"))
      res(1).inputLabels should be(List("test_1", "test_2"))
      res(2).inputLabels should be(List("test_3", "test_2"))
      res(3).inputLabels should be(List("test_1"))
    }

    it("multiple inputs, multiple actions, some are empty") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addInput("test_2", Some("value_2"))
        .addInput("test_3", None)
        .addInput("test_4", Some("value_4"))
        .addAction(new TestEmptyAction(List("test_1"), List("test_21")))
        .addAction(new TestEmptyAction(List("test_1", "test_2"), List("test_22")))
        .addAction(new TestEmptyAction(List("test_3", "test_2"), List("test_23")))
        .addAction(new TestEmptyAction(List("test_1"), List("test_24")))

      val res = flow.nextRunnable(defaultPool)
      res.size should be(3)
      res(0).inputLabels should be(List("test_1"))
      res(1).inputLabels should be(List("test_1", "test_2"))
      res(2).inputLabels should be(List("test_1"))
    }

    // nextRunnable tests for tag dependencies
    it("two non-empty inputs, two actions, all ready") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addInput("test_2", Some("value_2"))
        .addAction(new TestEmptyAction(List("test_1"), List.empty))
        .addAction(new TestEmptyAction(List("test_2"), List.empty))

      val res = flow.nextRunnable(defaultPool)
      res.size should be(2)
      res(0).inputLabels should be(List("test_1"))
      res(1).inputLabels should be(List("test_2"))
    }

    it("multiple non-empty inputs, multiple actions, all ready, action is tagged but no dependency") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addInput("test_2", Some("value_2"))
        .tag("tag1") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
        .addAction(new TestEmptyAction(List("test_2"), List.empty))

      val res = flow.nextRunnable(defaultPool)
      res.size should be(2)
      res(0).inputLabels should be(List("test_1"))
      res(1).inputLabels should be(List("test_2"))
    }

    it("multiple non-empty inputs, multiple actions, all ready, tagged with empty tags") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addInput("test_2", Some("value_2"))
        .tag() {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
        .tagDependency() {
          _.addAction(new TestEmptyAction(List("test_2"), List.empty))
        }

      val res = flow.nextRunnable(defaultPool)
      res.size should be(2)
      res(0).inputLabels should be(List("test_1"))
      res(1).inputLabels should be(List("test_2"))
    }

    it("multiple non-empty inputs, multiple actions, all ready, dependency between actions") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addInput("test_2", Some("value_2"))
        .tag("tag1") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
        .tagDependency("tag1") {
          _.addAction(new TestEmptyAction(List("test_2"), List.empty))
        }

      val res = flow.nextRunnable(defaultPool)
      res.size should be(1)
      res(0).inputLabels should be(List("test_1"))
    }

    it("multiple non-empty inputs, multiple actions, all ready, dependency between actions, different pools defined inside") {
      val emptyFlow = MockDataFlow.empty
      val action_1 = new TestEmptyAction(List("test_1"), List.empty)
      val action_2 = new TestEmptyAction(List("test_2"), List.empty)
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addInput("test_2", Some("value_2"))
        .tag("tag1") {
          _.executionPool("p1") { _.addAction(action_1)}
        }
        .tagDependency("tag1") {
          _.executionPool("p2") { _.addAction(action_2) }
        }

      flow.schedulingMeta.actionState.size should be(2)
      flow.schedulingMeta.actionState(action_1.schedulingGuid).executionPoolName should be("p1")
      flow.schedulingMeta.actionState(action_2.schedulingGuid).executionPoolName should be("p2")

      val res = flow.nextRunnable(Set("p1"))
      res.size should be(1)
      res(0).inputLabels should be(List("test_1"))

      flow.nextRunnable(Set("p2")) should be(Seq.empty)
    }

    it("multiple non-empty inputs, multiple actions, all ready, dependency between actions tagged twice with different tags") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addInput("test_2", Some("value_2"))
        .tag("tag1", "tag2") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
        .tagDependency("tag1", "tag2") {
          _.addAction(new TestEmptyAction(List("test_2"), List.empty))
        }

      val res = flow.nextRunnable(defaultPool)
      res.size should be(1)
      res(0).inputLabels should be(List("test_1"))
    }

    it("one empty input, multiple actions, only dependent action is ready") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", None)
        .addInput("test_2", Some("value_2"))
        .tag("tag1") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
        .tagDependency("tag1") {
          _.addAction(new TestEmptyAction(List("test_2"), List.empty))
        }

      val res = flow.nextRunnable(defaultPool)
      res.size should be(0)

    }

    it("one action that is ready, dependent tag but no dependent actions present (simulates midflow)") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_2", Some("value_2"))
        .tagDependency("tag1") {
          _.addAction(new TestEmptyAction(List("test_2"), List.empty))
        }

      val res = flow.nextRunnable(defaultPool)
      res.size should be(1)
      res(0).inputLabels should be(List("test_2"))
    }

  }

  describe("prepareForExecution") {

    it("add action with non existing input") {
      val emptyFlow = MockDataFlow.empty
      val res = intercept[DataFlowException] {
        emptyFlow.addAction(new TestEmptyAction(List("table_1"), List.empty)).prepareForExecution().get
      }
      res.text should be(s"Input label [table_1] is not produced by any previous actions")
    }

    it("add action, with non existing input 2") {
      val emptyFlow = MockDataFlow.empty
      val res = intercept[DataFlowException] {
        emptyFlow
          .addInput("test_1", Some("value_1"))
          .addAction(new TestEmptyAction(List("test_2"), List.empty)).prepareForExecution().get
      }
      res.text should be(s"Input label [test_2] is not produced by any previous actions")
    }

    it("cyclic dependency on input labels") {
      val emptyFlow = MockDataFlow.empty
      val res = intercept[DataFlowException] {
        emptyFlow
          .addAction(new TestEmptyAction(List("test_2"), List("test_1")) {
            override val guid = "action1"
          })
          .addAction(new TestEmptyAction(List("test_1"), List("test_2")) {
            override val guid = "action2"
          }).prepareForExecution().get
      }
      res.text should be("Circular reference for input label(s) [test_1] when resolving action [action2]. " +
        "Action uses input labels that itself, a sub-action or tag-dependent sub-action outputs.")
    }

    it("duplicate existing label") {
      val emptyFlow = MockDataFlow.empty
      val res = intercept[DataFlowException] {
        emptyFlow
          .addInput("test_1", Some("value_1"))
          .addInput("test_1", Some("value_1")).prepareForExecution().get
      }
      res.text should be(s"Input label [test_1] already exists")
    }

    it("add input label with same name as existing output label") {
      val emptyFlow = MockDataFlow.empty
      val res = intercept[DataFlowException] {
        emptyFlow
          .addAction(new TestEmptyAction(List("test_1"), List("test_3")))
          .addInput("test_1", Some("value_1"))
          .addInput("test_3", Some("value_3")).prepareForExecution().get
      }
      res.text should be(s"Duplicate output labels found: The following labels were found as outputs to multiple actions and/or were in existing flow inputs: test_3")
    }

    it("one action with tag but no dependency") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_2", Some("value_2"))
        .tag("tag1") {
          _.addAction(new TestEmptyAction(List("test_2"), List.empty))
        }

      flow.prepareForExecution().get

    }

    it("one action that is missing a dependent tag") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_2", Some("value_2"))
        .tagDependency("tag1") {
          _.addAction(new TestEmptyAction(List("test_2"), List.empty) {
            override val guid = "action1"
          })
        }

      val res = intercept[DataFlowException] {
        flow.prepareForExecution().get
      }
      res.text should be("Could not find any actions tagged with label [tag1] when resolving dependent actions for action [action1]")

    }

    it("one action that has a tag dependency on itself") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_2", Some("value_2"))
        .tagDependency("tag1") {
          _.tag("tag1") {
            _.addAction(new TestEmptyAction(List("test_2"), List.empty) {
              override val guid = "action1"
            })
          }
        }

      val res = intercept[DataFlowException] {
        flow.prepareForExecution().get
      }
      res.text should be("Circular reference for action [action1] as a result of cyclic tag dependency. " +
        "Action has the following tag dependencies [tag1] and depends on the following input labels [test_2]")

    }

    it("execution whilst in a tag block") {
      val emptyFlow = MockDataFlow.empty

      val res = intercept[DataFlowException] {
        val f = emptyFlow
          .addInput("test_2", Some("value_2"))
          .tag("tag1") {
            _.addAction(new TestEmptyAction(List("test_2"), List.empty))
              .prepareForExecution().get
          }
      }
      res.text should be("Attempted to execute a flow whilst inside the following tag blocks: [tag1]")

    }

    it("execution whilst in a tagDependency block") {
      val emptyFlow = MockDataFlow.empty

      val res = intercept[DataFlowException] {
        emptyFlow
          .addInput("test_2", Some("value_2"))
          .tagDependency("tag1") {
            _.addAction(new TestEmptyAction(List("test_2"), List.empty))
              .prepareForExecution().get
          }
      }
      res.text should be("Attempted to execute a flow whilst inside the following tag dependency blocks: [tag1]")

    }

    it("three actions with tree dependency") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addInput("test_2", Some("value_2"))
        .addInput("test_3", Some("value_3"))
        .tag("tag1") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
        .tagDependency("tag1") {
          _.tag("tag2") {
            _.addAction(new TestEmptyAction(List("test_2"), List.empty))
          }
        }
        .tagDependency("tag2") {
          _.addAction(new TestEmptyAction(List("test_3"), List.empty))
        }

      flow.prepareForExecution().get shouldBe a[flow.type]

    }

    it("four actions with duplicate tag and dependency blocks") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .tag("tag1") {
          _.tag("tag1") {
            _.addAction(new TestEmptyAction(List("test_1"), List.empty))
          }
            .addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
        .tagDependency("tag1") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
            .tagDependency("tag1") {
              _.addAction(new TestEmptyAction(List("test_1"), List.empty))
            }
        }

      flow.tagState.taggedActions.count(_._2.tags.contains("tag1")) should be(2)
      flow.tagState.taggedActions.count(_._2.dependentOnTags.contains("tag1")) should be(2)
      flow.prepareForExecution().get shouldBe a[flow.type]

    }

    it("four actions, two initial, a third that depends on the first two, and a final one that depends on the third") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .tag("tag1") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
        .tag("tag2") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
        .tagDependency("tag1", "tag2") {
          _.tag("tag3") {
            _.addAction(new TestEmptyAction(List("test_1"), List.empty))
          }
        }
        .tagDependency("tag3") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
      flow.prepareForExecution().get shouldBe a[flow.type]
    }

    it("four actions, two initial, a third that depends on the first two, and a final one that depends all the given tags") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .tag("tag1") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
        .tag("tag2") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
        .tagDependency("tag1", "tag2") {
          _.tag("tag3") {
            _.addAction(new TestEmptyAction(List("test_1"), List.empty))
          }
        }
        .tagDependency("tag1", "tag2", "tag3") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty))
        }
      flow.prepareForExecution().get shouldBe a[flow.type]
    }

    it("three actions with circular dependency") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .addInput("test_2", Some("value_2"))
        .addInput("test_3", Some("value_3"))
        .tagDependency("tag3") {
          _.tag("tag1") {
            _.addAction(new TestEmptyAction(List("test_1"), List.empty) {
              override val guid = "action1"
            })
          }
        }
        .tagDependency("tag1") {
          _.tag("tag2") {
            _.addAction(new TestEmptyAction(List("test_2"), List.empty) {
              override val guid = "action2"
            })
          }
        }
        .tagDependency("tag2") {
          _.tag("tag3") {
            _.addAction(new TestEmptyAction(List("test_3"), List.empty) {
              override val guid = "action3"
            })
          }
        }

      val res = intercept[DataFlowException] {
        flow.prepareForExecution().get
      }
      res.text should be("Circular reference for action [action2] as a result of cyclic tag dependency. " +
        "Action has the following tag dependencies [tag1] and depends on the following input labels [test_2]")
    }

    it("four actions, two initial, a third that depends on the first two, and a final one that depends on the third " +
      "but produces an output needed by the first (cyclic dependency)") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .tag("tag1") {
          _.addAction(new TestEmptyAction(List("test_2"), List.empty) {
            override val guid = "action1"
          })
        }
        .tag("tag2") {
          _.addAction(new TestEmptyAction(List("test_1"), List.empty) {
            override val guid = "action2"
          })
        }
        .tagDependency("tag1", "tag2") {
          _.tag("tag3") {
            _.addAction(new TestEmptyAction(List("test_1"), List.empty) {
              override val guid = "action3"
            })
          }
        }
        .tagDependency("tag3") {
          _.addAction(new TestEmptyAction(List("test_1"), List("test_2")) {
            override val guid = "action4"
          })
        }

      val res = intercept[DataFlowException] {
        flow.prepareForExecution().get shouldBe a[flow.type]
      }
      res.text should be("Circular reference for action [action3] as a result of cyclic tag dependency. " +
        "Action has the following tag dependencies [tag1, tag2] and depends on the following input labels [test_1]")
    }

    it("three actions with circular dependency on label and tag") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .addInput("test_1", Some("value_1"))
        .tagDependency("tag3") {
          _.addAction(new TestEmptyAction(List("test_1"), List("test_2")) {
            override val guid = "action1"
          })
        }
        .addAction(new TestEmptyAction(List("test_2"), List("test_3")) {
          override val guid = "action2"
        })
        .tag("tag3") {
          _.addAction(new TestEmptyAction(List("test_3"), List.empty) {
            override val guid = "action3"
          })
        }

      val res = intercept[DataFlowException] {
        flow.prepareForExecution().get
      }
      res.text should be("Circular reference for input label(s) [test_2] when resolving action [action2]. " +
        "Action uses input labels that itself, a sub-action or tag-dependent sub-action outputs.")
    }


  }

  describe("executed actions") {

    describe("failures") {

      it("one output, action produced no output") {
        val emptyFlow = MockDataFlow.empty
        val action = new TestEmptyAction(List.empty, List("table_1"))
        val flow = emptyFlow.addAction(action)
        val res = intercept[DataFlowException] {
          flow.executed(action, Seq.empty)
        }
        res.text should be(s"Action produced different number of results. Expected 1, but was 0. ${action.guid}: Action: TestEmptyAction Inputs: [] Outputs: [table_1]")
      }

      it("one output, action produced more outputs") {
        val emptyFlow = MockDataFlow.empty
        val action = new TestEmptyAction(List.empty, List("table_1"))
        val flow = emptyFlow.addAction(action)
        val res = intercept[DataFlowException] {
          flow.executed(action, Seq(Some("v_1"), Some("v2")))
        }
        res.text should be(s"Action produced different number of results. Expected 1, but was 2. ${action.guid}: Action: TestEmptyAction Inputs: [] Outputs: [table_1]")
      }

      it("2 outputs, action only one is produced") {
        val emptyFlow = MockDataFlow.empty
        val action = new TestEmptyAction(List.empty, List("table_1", "table_2"))
        val flow = emptyFlow.addAction(action)
        val res = intercept[DataFlowException] {
          flow.executed(action, Seq(Some("v1")))
        }
        res.text should be(s"Action produced different number of results. Expected 2, but was 1. ${action.guid}: Action: TestEmptyAction Inputs: [] Outputs: [table_1,table_2]")
      }

    }

    describe("success") {

      it("flow with one action, no output, pre existing inputs") {
        val emptyFlow = MockDataFlow.empty
        val action = new TestEmptyAction(List("t_1"), List.empty)
        val flow = emptyFlow.addInput("t_1", Some("v1")).addAction(action)
        flow.actions.size should be(1)
        val resFlow = flow.executed(action, Seq.empty)
        resFlow.actions.size should be(0)
        resFlow.inputs should be(DataFlowEntities(Map("t_1" -> Some("v1")))) // original inputs are still there
      }

      it("flow with one action, one output, no pre existing inputs, empty") {
        val emptyFlow = MockDataFlow.empty
        val action = new TestEmptyAction(List.empty, List("table_1"))
        val flow = emptyFlow.addAction(action)
        val resFlow = flow.executed(action, Seq(None))
        resFlow.actions.size should be(0)
        resFlow.inputs should be(DataFlowEntities(Map("table_1" -> None)))
      }

      it("flow with one action, one output, no pre existing inputs, not empty") {
        val emptyFlow = MockDataFlow.empty
        val action = new TestEmptyAction(List.empty, List("table_1"))
        val flow = emptyFlow.addAction(action)
        val resFlow = flow.executed(action, Seq(Some("v1")))
        resFlow.actions.size should be(0)
        resFlow.inputs should be(DataFlowEntities(Map("table_1" -> Some("v1"))))
      }

      it("flow with one action, 3 outputs, no pre existing inputs") {
        val emptyFlow = MockDataFlow.empty
        val action = new TestEmptyAction(List.empty, List("t_1", "t_2", "t_3"))
        val flow = emptyFlow.addAction(action)
        val resFlow = flow.executed(action, Seq(Some("v1"), None, Some("v3")))
        resFlow.actions.size should be(0)
        resFlow.inputs should be(DataFlowEntities(Map("t_1" -> Some("v1"), "t_2" -> None, "t_3" -> Some("v3"))))
      }

      it("flow with one action, 3 outputs, with pre existing inputs") {
        val emptyFlow = MockDataFlow.empty
        val action = new TestEmptyAction(List.empty, List("t_1", "t_2", "t_3"))
        val flow = emptyFlow
          .addInput("t_0", Some("v0"))
          .addInput("t_00", Some("v00"))
          .addAction(action)
        val resFlow = flow.executed(action, Seq(Some("v1"), None, Some("v3")))
        resFlow.actions.size should be(0)
        resFlow.inputs should be(DataFlowEntities(Map("t_0" -> Some("v0"), "t_00" -> Some("v00"), "t_1" -> Some("v1"), "t_2" -> None, "t_3" -> Some("v3"))))
      }

      it("flow with 3 actions, 3 outputs, with pre existing inputs") {
        val emptyFlow = MockDataFlow.empty
        val action_1 = new TestEmptyAction(List.empty, List("t_1"))
        val action_2 = new TestEmptyAction(List.empty, List("t_2"))
        val action_3 = new TestEmptyAction(List.empty, List("t_3"))
        val action = new TestEmptyAction(List("t_1", "t_2", "t_3"), List.empty)
        val flow = emptyFlow
          .addInput("t_0", Some("v0"))
          .addInput("t_00", Some("v00"))
          .addAction(action_1)
          .addAction(action_2)
          .addAction(action_3)
          .addAction(action)
        flow.actions.size should be(4)

        val resFlow_1 = flow.executed(action_1, Seq(Some("v1")))
        resFlow_1.actions.size should be(3)
        resFlow_1.inputs should be(DataFlowEntities(Map("t_0" -> Some("v0"), "t_00" -> Some("v00"), "t_1" -> Some("v1"))))

        val resFlow_2 = resFlow_1.executed(action_2, Seq(Some("v2")))
        resFlow_2.actions.size should be(2)
        resFlow_2.inputs should be(DataFlowEntities(Map("t_0" -> Some("v0"), "t_00" -> Some("v00"), "t_1" -> Some("v1"), "t_2" -> Some("v2"))))

        val resFlow_3 = resFlow_2.executed(action_3, Seq(Some("v3")))
        resFlow_3.actions.size should be(1)
        resFlow_3.inputs should be(DataFlowEntities(Map("t_0" -> Some("v0"), "t_00" -> Some("v00"), "t_1" -> Some("v1"), "t_2" -> Some("v2"), "t_3" -> Some("v3"))))

        val resFlow = resFlow_3.executed(action, Seq.empty)
        resFlow.actions.size should be(0)
        resFlow.inputs should be(DataFlowEntities(Map("t_0" -> Some("v0"), "t_00" -> Some("v00"), "t_1" -> Some("v1"), "t_2" -> Some("v2"), "t_3" -> Some("v3"))))
      }

    }

  }

  describe("map/mapOption") {

    it("map should transform a dataflow") {

      val emptyFlow = MockDataFlow.empty
      emptyFlow.actions.map(_.guid) should be(Seq())

      val mappedFlow = emptyFlow.map { f =>
        f.addAction(new TestEmptyAction(List.empty, List.empty) {
          override val guid: String = "abd22c36-4dd0-4fa5-9298-c494ede7f363"
        })
      }
      mappedFlow.actions.map(_.guid) should be(Seq("abd22c36-4dd0-4fa5-9298-c494ede7f363"))
    }

    it("mapOption should transform a dataflow") {

      val emptyFlow = MockDataFlow.empty
      emptyFlow.actions.map(_.guid) should be(Seq())

      val noneMappedFlow = emptyFlow.mapOption(_ => None)
      noneMappedFlow.actions.map(_.guid) should be(Seq())

      val someMappedFlow = emptyFlow.mapOption { f =>
        Some(f.addAction(new TestEmptyAction(List.empty, List.empty) {
          override val guid: String = "abd22c36-4dd0-4fa5-9298-c494ede7f363"
        }))
      }
      someMappedFlow.actions.map(_.guid) should be(Seq("abd22c36-4dd0-4fa5-9298-c494ede7f363"))
    }

    it("map should transform a dataflow when using implicit classes") {

      val emptyFlow = MockDataFlow.empty

      implicit class TestImplicit(dataFlow: MockDataFlow) {
        def runTest: MockDataFlow = dataFlow.addAction(new TestEmptyAction(List.empty, List.empty) {
          override val guid: String = "abd22c36-4dd0-4fa5-9298-c494ede7f363"
        })
      }

      emptyFlow.map(f => f.runTest).actions.map(_.guid) should be(Seq("abd22c36-4dd0-4fa5-9298-c494ede7f363"))

    }

  }

  describe("Execution Pools") {

    val action_1 = new TestEmptyAction(List.empty, List("t_1"))
    val action_2 = new TestEmptyAction(List.empty, List("t_2"))
    val action_3 = new TestEmptyAction(List.empty, List("t_3"))
    val action_4 = new TestEmptyAction(List("t_1", "t_2", "t_3"), List.empty)
    val action_5 = new TestEmptyAction(List("t_1"), List.empty)

    val appendFunc = (in: Option[String]) => in.map(_ + "_6789")

    it("default execution pool") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
                    .addAction(action_1)
                    .addAction(action_2)
                    .addAction(action_3)
                    .addAction(action_4)

      flow.schedulingMeta.actionState.size should be(4)
      flow.schedulingMeta.actionState(action_1.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
      flow.schedulingMeta.actionState(action_2.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
      flow.schedulingMeta.actionState(action_3.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
      flow.schedulingMeta.actionState(action_4.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)

      val runnable = flow.nextRunnable(Set(DEFAULT_POOL_NAME))
      runnable.size should be(3)
    }

    it("interceptor does not change the original scheduling guid") {
      val emptyFlow = MockDataFlow.empty
      val post = new PostActionInterceptor[String](action_2, Seq(TransformPostAction(appendFunc, "t_2")))

      val flow = emptyFlow
        .addAction(action_1)
        .addAction(action_2)
        .addAction(action_3)
        .addAction(action_4)
        .addInterceptor(post, action_2.guid)

      flow.schedulingMeta.actionState.size should be(4)
      flow.schedulingMeta.actionState(action_1.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
      flow.schedulingMeta.actionState(action_2.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
      flow.schedulingMeta.actionState(action_3.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
      flow.schedulingMeta.actionState(action_4.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
    }

    it("non default execution pool") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow
        .executionPool("first_pool") {
          _.addAction(action_1)
          .addAction(action_2)
          .addAction(action_3)
          .addAction(action_4)
      }

      flow.schedulingMeta.actionState.size should be(4)
      flow.schedulingMeta.actionState(action_1.schedulingGuid).executionPoolName should be("first_pool")
      flow.schedulingMeta.actionState(action_2.schedulingGuid).executionPoolName should be("first_pool")
      flow.schedulingMeta.actionState(action_3.schedulingGuid).executionPoolName should be("first_pool")
      flow.schedulingMeta.actionState(action_4.schedulingGuid).executionPoolName should be("first_pool")

      flow.nextRunnable(Set(DEFAULT_POOL_NAME)).isEmpty should be(true)
      val runnable = flow.nextRunnable(Set("first_pool")).map(_.schedulingGuid)
      runnable.size should be(3)
      runnable.contains(action_1.schedulingGuid) should be(true)
      runnable.contains(action_2.schedulingGuid) should be(true)
      runnable.contains(action_3.schedulingGuid) should be(true)

    }

    describe("Multiple Execution pools") {

      it("first wave is in default pool") {
        val emptyFlow = MockDataFlow.empty
        val flow = emptyFlow
          .addAction(action_1)
          .addAction(action_2)
          .addAction(action_3)
          .executionPool("first_pool") {
            _.addAction(action_4)
          }

        flow.schedulingMeta.actionState.size should be(4)
        flow.schedulingMeta.actionState(action_1.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
        flow.schedulingMeta.actionState(action_2.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
        flow.schedulingMeta.actionState(action_3.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
        flow.schedulingMeta.actionState(action_4.schedulingGuid).executionPoolName should be("first_pool")
      }
    }

    it("second wave is in default pool") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow.executionPool("first_pool") {
          _.addAction(action_1)
            .addAction(action_2)
            .addAction(action_3)
        }
        .addAction(action_4)

      flow.schedulingMeta.actionState.size should be(4)
      flow.schedulingMeta.actionState(action_1.schedulingGuid).executionPoolName should be("first_pool")
      flow.schedulingMeta.actionState(action_2.schedulingGuid).executionPoolName should be("first_pool")
      flow.schedulingMeta.actionState(action_3.schedulingGuid).executionPoolName should be("first_pool")
      flow.schedulingMeta.actionState(action_4.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
    }

    it("nested pools") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow.executionPool("first_pool") {
        _.addAction(action_1)
          .executionPool("second_pool") {
            _.addAction(action_2).executionPool("third_pool") {
              _.addAction(action_3)
            }
          }.addAction(action_5)

      }.addAction(action_4)

      flow.schedulingMeta.actionState.size should be(5)
      flow.schedulingMeta.actionState(action_1.schedulingGuid).executionPoolName should be("first_pool")
      flow.schedulingMeta.actionState(action_2.schedulingGuid).executionPoolName should be("second_pool")
      flow.schedulingMeta.actionState(action_3.schedulingGuid).executionPoolName should be("third_pool")
      flow.schedulingMeta.actionState(action_4.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
      flow.schedulingMeta.actionState(action_5.schedulingGuid).executionPoolName should be("first_pool")

      val allPools = flow.nextRunnable(Set(DEFAULT_POOL_NAME, "first_pool", "second_pool", "third_pool")).map(_.schedulingGuid)
      allPools.size should be(3)
      allPools.intersect(Seq(action_1.schedulingGuid, action_2.schedulingGuid, action_3.schedulingGuid)).size should be(3)

      val defaultPool = flow.nextRunnable(Set(DEFAULT_POOL_NAME)).map(_.schedulingGuid)
      defaultPool.isEmpty should be(true)

      val firstSecondPool = flow.nextRunnable(Set("first_pool", "second_pool")).map(_.schedulingGuid)
      firstSecondPool.size should be(2)
      firstSecondPool.intersect(Seq(action_1.schedulingGuid, action_2.schedulingGuid)).size should be(2)
    }

    it("nested pools and tags") {
      val emptyFlow = MockDataFlow.empty
      val flow = emptyFlow.executionPool("first_pool") {
        _.tag("t1") {
          _.addAction(action_1)
        }.executionPool("second_pool") {
            _.addAction(action_2).executionPool("third_pool") {
              _.tagDependency("t1") {
                _.addAction(action_3)
              }
            }
          }.addAction(action_5)

      }.addAction(action_4)

      flow.schedulingMeta.actionState.size should be(5)
      flow.schedulingMeta.actionState(action_1.schedulingGuid).executionPoolName should be("first_pool")
      flow.schedulingMeta.actionState(action_2.schedulingGuid).executionPoolName should be("second_pool")
      flow.schedulingMeta.actionState(action_3.schedulingGuid).executionPoolName should be("third_pool")
      flow.schedulingMeta.actionState(action_4.schedulingGuid).executionPoolName should be(DEFAULT_POOL_NAME)
      flow.schedulingMeta.actionState(action_5.schedulingGuid).executionPoolName should be("first_pool")

      val allPools = flow.nextRunnable(Set(DEFAULT_POOL_NAME, "first_pool", "second_pool", "third_pool")).map(_.schedulingGuid)
      allPools.size should be(2)
      allPools.intersect(Seq(action_1.schedulingGuid, action_2.schedulingGuid)).size should be(2) // no third action due to tag

      val defaultPool = flow.nextRunnable(Set(DEFAULT_POOL_NAME)).map(_.schedulingGuid)
      defaultPool.isEmpty should be(true)

      val firstSecondPool = flow.nextRunnable(Set("first_pool", "second_pool")).map(_.schedulingGuid)
      firstSecondPool.size should be(2)
      firstSecondPool.intersect(Seq(action_1.schedulingGuid, action_2.schedulingGuid)).size should be(2)
    }
  }

  describe("Commits configuration") {

    val action_1 = new TestEmptyAction(List.empty, List("t_1"))
    val action_2 = new TestEmptyAction(List.empty, List("t_2"))
    val action_3 = new TestEmptyAction(List.empty, List("t_3"))
    val action_4 = new TestEmptyAction(List("t_1", "t_2", "t_3"), List("com_1"))
    val action_5 = new TestEmptyAction(List("t_1"), List("com_2"))

    val emptyFlow = MockDataFlow.empty

    val dataCommitter = new TestDataCommitter()

    val flow = emptyFlow
      .addAction(action_1)
      .addAction(action_2)
      .addAction(action_3)
      .addAction(action_4)
      .addAction(action_5)

    it("no commits") {
      flow.commitMeta.commits.isEmpty should be(true)
      flow.commitMeta.pushes.isEmpty should be(true)
    }

    describe("one commit, no push") {

      it("one label, no partitions") {
        val testFlow = flow.commit("commit_1")("com_1")
        testFlow.commitMeta.commits.size should be(1)
        testFlow.commitMeta.commits.get("commit_1") should be(Some(Seq(CommitEntry("com_1", "commit_1", None, repartition = false, cache = true))))
        testFlow.commitMeta.pushes.isEmpty should be(true)
        testFlow.tagState.activeTags.isEmpty should be(true)
      }

      it("one label, partitions, no repartition") {
        val testFlow = flow.commit("commit_1", Seq("id", "cntry"), repartition = false)("com_1")
        testFlow.commitMeta.commits.size should be(1)
        testFlow.commitMeta.commits.get("commit_1") should be(Some(Seq(CommitEntry("com_1", "commit_1", Some(Left(Seq("id", "cntry"))), repartition = false, cache = true))))
        testFlow.commitMeta.pushes.isEmpty should be(true)
        testFlow.tagState.activeTags.isEmpty should be(true)
      }

      it("one label, partitions, repartition") {
        val testFlow = flow.commit("commit_1", Seq("id", "cntry"))("com_1")
        testFlow.commitMeta.commits.size should be(1)
        testFlow.commitMeta.commits.get("commit_1") should be(Some(Seq(CommitEntry("com_1", "commit_1", Some(Left(Seq("id", "cntry"))), repartition = true, cache = true))))
        testFlow.commitMeta.pushes.isEmpty should be(true)
        testFlow.tagState.activeTags.isEmpty should be(true)
      }

      it("one label, partitions, repartition, no cache") {
        flow.flowContext.asInstanceOf[EmptyFlowContext].conf.setProperty(CACHE_REUSED_COMMITTED_LABELS, "false")
        val testFlow = flow.commit("commit_1", Seq("id", "cntry"))("com_1")
        testFlow.commitMeta.commits.size should be(1)
        testFlow.commitMeta.commits.get("commit_1") should be(Some(Seq(CommitEntry("com_1", "commit_1", Some(Left(Seq("id", "cntry"))), repartition = true, cache = false))))
        testFlow.commitMeta.pushes.isEmpty should be(true)
        testFlow.tagState.activeTags.isEmpty should be(true)
      }
    }

    describe("pushes, no commits") {

      it("one push") {
        val testFlow = flow.push("commit_1")(dataCommitter)
        testFlow.commitMeta.commits.isEmpty should be(true)
        testFlow.tagState.activeTags.isEmpty should be(true)
        testFlow.commitMeta.pushes.keySet should be(Set("commit_1"))
        testFlow.commitMeta.pushes("commit_1").size should be(1)
      }

      it("one push, accept multiple committers") { //it should accept, but it will fail validation
        val testFlow = flow
          .push("commit_1")(dataCommitter)
          .push("commit_1")(dataCommitter)
        testFlow.commitMeta.commits.isEmpty should be(true)
        testFlow.tagState.activeTags.isEmpty should be(true)
        testFlow.commitMeta.pushes.keySet should be(Set("commit_1"))
        testFlow.commitMeta.pushes("commit_1").size should be(2)
      }

      it("2 pushes") {
        val testFlow = flow
          .push("commit_1")(dataCommitter)
          .push("commit_2")(dataCommitter)
        testFlow.commitMeta.commits.isEmpty should be(true)
        testFlow.tagState.activeTags.isEmpty should be(true)
        testFlow.commitMeta.pushes.keySet should be(Set("commit_1", "commit_2"))
        testFlow.commitMeta.pushes("commit_1").size should be(1)
        testFlow.commitMeta.pushes("commit_2").size should be(1)
      }

    }

    describe("build committers, no validations") {

      it("single commit, single label") {
        val testFlow = emptyFlow
          .commit("commit_1")("label_1")
          .push("commit_1")(dataCommitter)
          .buildCommits()

        testFlow.actions.size should be(3)
        testFlow.tagState.taggedActions.keySet should be(testFlow.actions.map(_.guid).toSet)

        val cacheActions = testFlow.tagState.taggedActions.filter(_._2.tags.contains("commit_1")).values.toList
        cacheActions.size should be(1)
        cacheActions(0).tags should be(Set("commit_1"))
        cacheActions(0).dependentOnTags should be(Set.empty)

        val moveActions = testFlow.tagState.taggedActions.filter(_._2.tags.contains("commit_1_AFTER_COMMIT")).values.toList
        moveActions.size should be(1)
        moveActions(0).tags should be(Set("commit_1_AFTER_COMMIT"))
        moveActions(0).dependentOnTags should be(Set("commit_1"))

        val finishActions = testFlow.tagState.taggedActions.filter(_._2.dependentOnTags.contains("commit_1_AFTER_COMMIT")).values.toList
        finishActions.size should be(1)
        finishActions(0).tags should be(Set.empty)
        finishActions(0).dependentOnTags should be(Set("commit_1_AFTER_COMMIT"))
      }

      it("multiple commits, single label") {
        val testFlow = emptyFlow
          .commit("commit_1")("label_1")
          .push("commit_1")(dataCommitter)
          .push("commit_2")(dataCommitter)
          .commit("commit_2")("label_2")
          .buildCommits()

        testFlow.actions.size should be(6)
        testFlow.tagState.taggedActions.keySet should be(testFlow.actions.map(_.guid).toSet)

        val cacheActions_1 = testFlow.tagState.taggedActions.filter(_._2.tags.contains("commit_1")).values.toList
        cacheActions_1.size should be(1)
        cacheActions_1.head.tags should be(Set("commit_1"))
        cacheActions_1.head.dependentOnTags should be(Set.empty)

        val moveActions_1 = testFlow.tagState.taggedActions.filter(_._2.tags.contains("commit_1_AFTER_COMMIT")).values.toList
        moveActions_1.size should be(1)
        moveActions_1.head.tags should be(Set("commit_1_AFTER_COMMIT"))
        moveActions_1.head.dependentOnTags should be(Set("commit_1"))

        val finishActions_1 = testFlow.tagState.taggedActions.filter(_._2.dependentOnTags.contains("commit_1_AFTER_COMMIT")).values.toList
        finishActions_1.size should be(1)
        finishActions_1.head.tags should be(Set.empty)
        finishActions_1.head.dependentOnTags should be(Set("commit_1_AFTER_COMMIT"))

        val cacheActions_2 = testFlow.tagState.taggedActions.filter(_._2.tags.contains("commit_2")).values.toList
        cacheActions_2.size should be(1)
        cacheActions_2.head.tags should be(Set("commit_2"))
        cacheActions_2.head.dependentOnTags should be(Set.empty)

        val moveActions_2 = testFlow.tagState.taggedActions.filter(_._2.tags.contains("commit_2_AFTER_COMMIT")).values.toList
        moveActions_2.size should be(1)
        moveActions_2.head.tags should be(Set("commit_2_AFTER_COMMIT"))
        moveActions_2.head.dependentOnTags should be(Set("commit_2"))

        val finishActions_2 = testFlow.tagState.taggedActions.filter(_._2.dependentOnTags.contains("commit_2_AFTER_COMMIT")).values.toList
        finishActions_2.size should be(1)
        finishActions_2.head.tags should be(Set.empty)
        finishActions_2.head.dependentOnTags should be(Set("commit_2_AFTER_COMMIT"))
      }
    }

  }
}

class TestEmptyAction(val inputLabels: List[String], val outputLabels: List[String]) extends DataFlowAction {

  override def performAction[C <: FlowContext](inputs: DataFlowEntities, flowContext: C): Try[ActionResult] = Try(List.empty)

}

class TestDataCommitter extends DataCommitter[MockDataFlow] {

  override def stageToTempFlow(commitName: String, commitUUID: UUID, labels: Seq[CommitEntry], flow: MockDataFlow): MockDataFlow = {
    labels.map(_.label).foldLeft(flow) { (res, label) => res.addAction(new TestEmptyAction(List(label + "_input"), List(label + "_output"))) }
  }

  override def moveToPermanentStorageFlow(commitName: String, commitUUID: UUID, labels: Seq[CommitEntry], flow: MockDataFlow): MockDataFlow = {
    flow.addAction(new TestEmptyAction(labels.map(_.label + "_output").toList, List.empty))
  }

  override def finish(commitName: String, commitUUID: UUID, labels: Seq[CommitEntry], flow: MockDataFlow): MockDataFlow = {
    flow.addAction(new TestEmptyAction(labels.map(_.label + "_output").toList, List.empty))
  }

  override protected[dataflow] def validate(flow: MockDataFlow, commitName: String, entries: Seq[CommitEntry]): Try[Unit] = Success(Unit)

}