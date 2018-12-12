package com.coxautodata.waimak.dataflow

import org.scalatest.{FunSpec, Matchers}

import scala.util.Success

/**
  * Created by Alexei Perelighin on 2018/02/27
  */
class TestInterceptorAction extends FunSpec with Matchers {

  describe("smoke tests") {

    val action = new TestEmptyAction(List.empty, List("table_1"))

    it("different") {
      val interceptor = new InterceptorAction(action)
      interceptor.guid should not be(action.guid)
    }

    it("same") {
      val interceptor = new InterceptorAction(action)
      interceptor.requiresAllInputs should be(action.requiresAllInputs)
      interceptor.inputLabels should be(action.inputLabels)
      interceptor.outputLabels should be(action.outputLabels)
    }

  }

  describe("post interceptor") {

    val func2 = () => List(Some("v1"), Some("v2"))

    val func2None = () => List(Some("v1"), None)

    val emptyInputs = DataFlowEntities.empty

    val appendFunc = (in: Option[String]) => in.map(_ + "_6789")

    it("post first output") {
      val action = new TestPresetAction(List.empty, List("o1", "o2"), func2)
      val post = new PostActionInterceptor[String](action, Seq(TransformPostAction(appendFunc, "o1")))
      val res = post.performAction(emptyInputs, new EmptyFlowContext)

      res should be(Success(Seq(Some("v1_6789"), Some("v2"))))
    }

    it("post second output") {
      val action = new TestPresetAction(List.empty, List("o1", "o2"), func2)
      val post = new PostActionInterceptor[String](action, Seq(TransformPostAction(appendFunc, "o2")))
      val res = post.performAction(emptyInputs, new EmptyFlowContext)

      res should be(Success(Seq(Some("v1"), Some("v2_6789"))))
    }

    it("post None output") {
      val action = new TestPresetAction(List.empty, List("o1", "o2"), func2None)
      val post = new PostActionInterceptor[String](action, Seq(TransformPostAction(appendFunc, "o2")))
      val res = post.performAction(emptyInputs, new EmptyFlowContext)

      res should be(Success(Seq(Some("v1"), None)))
    }

    it("post non existing name") {
      val action = new TestPresetAction(List.empty, List("o1", "o2"), func2)
      val post = new PostActionInterceptor[String](action, Seq(TransformPostAction(appendFunc, "doesnotexist")))

      val res = intercept[DataFlowException] {
        post.performAction(emptyInputs, new EmptyFlowContext)
      }

      res.text should be(s"Can not apply post action to label doesnotexist, it does not exist in action ${action.guid}: Action: TestPresetAction Inputs: [] Outputs: [o1,o2].")
    }

    it("description test") {
      val action = new TestPresetAction(List.empty, List("o1", "o2"), func2None)
      val post = new PostActionInterceptor[String](action, Seq(TransformPostAction(appendFunc, "o2"), CachePostAction(null, "o1")))

      post.description should be("Action: PostActionInterceptor Inputs: [] Outputs: [o1,o2]"
        + "\nIntercepted Action: TestPresetAction Inputs: [] Outputs: [o1,o2]"
        + "\nIntercepted with: PostAction: TransformPostAction Label: o2, PostAction: CachePostAction Label: o1")
    }
  }

}
