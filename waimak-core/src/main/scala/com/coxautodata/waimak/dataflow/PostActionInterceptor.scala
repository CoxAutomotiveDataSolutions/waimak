package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging

import scala.util.Try

case class PostActionInterceptor[T](toIntercept: DataFlowAction
                                       , postActions: Seq[PostAction[T]])
  extends InterceptorAction(toIntercept) with Logging {

  override def instead(inputs: DataFlowEntities, flowContext: FlowContext): Try[ActionResult] = {
    val tryRes = intercepted.performAction(inputs, flowContext).map(_.toArray)
    tryRes.foreach { res =>
      postActions.groupBy(_.labelToIntercept).foreach {
        v =>
          val label = v._1
          val actionsForLabel = v._2
          val pos = intercepted.outputLabels.indexOf(label)
          if (pos < 0) throw new DataFlowException(s"Can not apply post action to label $label, it does not exist in action ${intercepted.logLabel}.")
          res(pos) = actionsForLabel.foldLeft(res(pos))((z, a) => a.run(z.map(_.asInstanceOf[T]), flowContext))
      }
    }
    tryRes.map(_.toList)
  }

  override def description: String =
       super.description + "\n" +
      "Intercepted " + toIntercept.description + "\n" +
      "Intercepted with: " + postActions.map(_.description).mkString(", ")

  def addPostAction(newAction: PostAction[T]): PostActionInterceptor[T] = newAction match {
    // Cache already exists, so ignore
    case CachePostAction(_, l) if postActions.exists(a => a.isInstanceOf[CachePostAction[T]] && a.labelToIntercept == l) =>
      logWarning(s"Label $l already has a cache interceptor, skipping")
      this
    // Cache exists, so make sure transform is before cache
    case TransformPostAction(_, l) if postActions.exists(a => a.isInstanceOf[CachePostAction[T]] && a.labelToIntercept == l) =>
      val (trans, cache) = postActions.partition(!_.isInstanceOf[CachePostAction[T]])
      val newActions = (trans :+ newAction) ++ cache
      PostActionInterceptor(toIntercept, newActions)
    // No cache exists yet
    case _ =>
      PostActionInterceptor(toIntercept, postActions :+ newAction)
  }

}

sealed abstract class PostAction[T](val labelToIntercept: String) {

  def run: (Option[T], FlowContext) => Option[T]

  def postActionName: String = getClass.getSimpleName

  def description = s"PostAction: $postActionName Label: ${labelToIntercept}"

}

sealed case class CachePostAction[T](run: (Option[T], FlowContext) => Option[T], override val labelToIntercept: String) extends PostAction[T](labelToIntercept)

sealed case class TransformPostAction[T](run: (Option[T], FlowContext) => Option[T], override val labelToIntercept: String) extends PostAction[T](labelToIntercept)