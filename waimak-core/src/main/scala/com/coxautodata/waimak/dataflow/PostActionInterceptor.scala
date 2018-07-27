package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging

import scala.util.Try

case class PostActionInterceptor[T, C](toIntercept: DataFlowAction[C]
                                       , postActions: Seq[PostAction[T, C]])
  extends InterceptorAction[C](toIntercept) with Logging {

  override def instead(inputs: DataFlowEntities, flowContext: C): Try[ActionResult] = {
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

  def addPostAction(newAction: PostAction[T, C]): PostActionInterceptor[T, C] = newAction match {
    // Cache already exists, so ignore
    case CachePostAction(_, l) if postActions.exists(a => a.isInstanceOf[CachePostAction[T, C]] && a.labelToIntercept == l) =>
      logWarning(s"Label $l already has a cache interceptor, skipping")
      this
    // Cache exists, so make sure transform is before cache
    case TransformPostAction(_, l) if postActions.exists(a => a.isInstanceOf[CachePostAction[T, C]] && a.labelToIntercept == l) =>
      val (trans, cache) = postActions.partition(!_.isInstanceOf[CachePostAction[T, C]])
      val newActions = (trans :+ newAction) ++ cache
      PostActionInterceptor(toIntercept, newActions)
    // No cache exists yet
    case _ =>
      PostActionInterceptor(toIntercept, postActions :+ newAction)
  }

}

sealed abstract class PostAction[T, C](val labelToIntercept: String) {
  def run: (Option[T], C) => Option[T]

  def postActionName: String = getClass.getSimpleName

  def description = s"PostAction: $postActionName Label: ${labelToIntercept}"
}

sealed case class CachePostAction[T, C](run: (Option[T], C) => Option[T], override val labelToIntercept: String) extends PostAction[T, C](labelToIntercept)

sealed case class TransformPostAction[T, C](run: (Option[T], C) => Option[T], override val labelToIntercept: String) extends PostAction[T, C](labelToIntercept)