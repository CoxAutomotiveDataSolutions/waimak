package org.apache.spark

import com.coxautodata.waimak.dataflow.DataFlow
import org.apache.spark.sql.execution.ui.{WaimakEdge, WaimakNode}

case class WaimakGraph (nodes: Seq[WaimakNode], edges: Seq[WaimakEdge]) {}

object WaimakGraph {
  def apply[T, C](flow: DataFlow[T, C]): WaimakGraph = {

    // Let's first sort the actions. The ones that do not have input dependencies should be at the top.
    val sortedActions = flow.actions.sortWith((a1, a2) => a1.inputLabels.length < a2.inputLabels.length).zipWithIndex

    val groupedActions = sortedActions.groupBy{case (a,i) => (flow.tagState.taggedActions(a.guid).tags, flow.tagState.taggedActions(a.guid).dependentOnTags)}

    var r = flow.actions.length -1

    val theNodes: Seq[WaimakNode] = groupedActions.flatMap{case ((k1, k2), l) =>
      if  (k1.isEmpty && k2.isEmpty) {
        l.map{case (a, i) => WaimakNode(i,{
          val str2 = {
            if (a.actionName.toLowerCase == a.actionName) a.actionName.toUpperCase
            else a.actionName
          }
          val str3 = if (str2.length < 11) "          " + str2 else str2

          str3 + s"\nInputs:         ${a.inputLabels mkString(", ")}"  + s"\nOutputs:      ${a.outputLabels mkString(", ")}"
        }
          , "")
        }
      }
      else {
        r += 1
        val str =
          if (k1.nonEmpty && k2.nonEmpty) s"Tags: ${k1 mkString(", ")} \n DependentOnTags: ${k2 mkString(", ")}"
          else if (k1.nonEmpty) s"Tags: ${k1 mkString(", ")}"
          else s"TagDependency: ${k2 mkString(", ")}"

        Seq(WaimakNode(r, str, "", l.map{case (a, i) => WaimakNode(i, {
          val str2 = {
            if (a.actionName.toLowerCase == a.actionName) a.actionName.toUpperCase
            else a.actionName
          }
          val str3 = if (str2.length < 11) "          " + str2 else str2

          str3 + s"\nInputs:         ${a.inputLabels mkString(", ")}" + s"\nOutputs:      ${a.outputLabels mkString(", ")}"
        }
          , "")}))
      }
    }.toSeq

    val theEdges: Seq[WaimakEdge] =
      for {
        (aa, i) <- sortedActions
        (ab, j) <- sortedActions
        if (aa.outputLabels.toSet.intersect(ab.inputLabels.toSet).nonEmpty
        || flow.tagState.taggedActions(aa.guid).tags.intersect(flow.tagState.taggedActions(ab.guid).dependentOnTags).nonEmpty)
      } yield WaimakEdge(i, j)

    WaimakGraph(theNodes1, theEdges)
  }
}