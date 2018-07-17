package org.apache.spark

import com.coxautodata.waimak.dataflow.{DataFlow, DataFlowAction}
import org.apache.spark.sql.execution.ui.{WaimakEdge, WaimakNode}

case class WaimakGraph (nodes: Seq[WaimakNode], edges: Seq[WaimakEdge]) {}

object WaimakGraph {
  def apply[T, C](flow: DataFlow[T, C]): WaimakGraph = {

    // Let's first sort the actions. The ones that do not have input dependencies should be at the top.
    val sortedActions = flow.actions.sortWith((a1, a2) => a1.inputLabels.length < a2.inputLabels.length).zipWithIndex

    // Group the actions by tags and tag's dependency so that are displayed in the same big box.
    val groupedActions: Map[(Set[String], Set[String]), Seq[(DataFlowAction[T, C], Int)]] = sortedActions.groupBy{case (a,i) =>
      (flow.tagState.taggedActions(a.guid).tags, flow.tagState.taggedActions(a.guid).dependentOnTags)}

    //This auxiliary function helps to produce an easier to read box capitalizing the action name when appropriate.
    def word(a : DataFlowAction[T, C]): String = {
      if (a.actionName.toLowerCase == a.actionName) a.actionName.toUpperCase
      else a.actionName
    }

    // Create the nodes to pass to WaimakGraph
    // (cnt is a counter to enumerate correctly the big boxes).
    val (_, theNodes): (Int, Seq[WaimakNode]) = groupedActions.foldLeft(flow.actions.length -> Seq[WaimakNode]()){

      case ((cnt, seq), ((s1, s2), l)) if s1.isEmpty && s2.isEmpty =>
        (cnt, seq ++ l.map{case (a, i) => WaimakNode(i,{
          word(a) + s"\nInputs:         ${a.inputLabels mkString ", " }" + s"\nOutputs:      ${a.outputLabels mkString ", " }"
        }, "")})

      case ((cnt, seq), ((s1, s2), l)) =>
        val str =
          if (s1.nonEmpty && s2.nonEmpty) s"Tags: ${s1 mkString ", " } \n DependentOnTags: ${s2 mkString ", " }"
          else if (s1.nonEmpty) s"Tags: ${s1 mkString ", " }"
          else s"TagDependency: ${s2 mkString ", " }"

        (cnt + 1, seq ++ Seq(WaimakNode(cnt, str, "", l.map{case (a, i) => WaimakNode(i, {
          word(a) + s"\nInputs:         ${a.inputLabels mkString ", " }" + s"\nOutputs:      ${a.outputLabels mkString ", " }"
        }, "")})))
    }

    // Create the edges to pass to WaimakGraph
    val theEdges: Seq[WaimakEdge] =
      for {
        (a, i) <- sortedActions
        (b, j) <- sortedActions
        if (a.outputLabels.toSet.intersect(b.inputLabels.toSet).nonEmpty
        || flow.tagState.taggedActions(a.guid).tags.intersect(flow.tagState.taggedActions(b.guid).dependentOnTags).nonEmpty)
      } yield WaimakEdge(i, j)

    WaimakGraph(theNodes, theEdges)
  }
}