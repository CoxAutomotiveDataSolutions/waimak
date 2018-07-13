package org.apache.spark

import com.coxautodata.waimak.dataflow.{DataFlow, DataFlowAction}
import org.apache.spark.sql.execution.ui.{WaimakEdge, WaimakNode}

case class WaimakGraph (nodes: Seq[WaimakNode], edges: Seq[WaimakEdge]) {}

object WaimakGraph {
  def apply[T, C](flow: DataFlow[T, C]): WaimakGraph = {

    // Let's first sort the actions. The ones that do not have input dependencies should be at the top.
    val sortedActions = flow.actions.sortWith((a1, a2) => a1.inputLabels.length < a2.inputLabels.length).zipWithIndex

    // Group the actions by tags and tag's dependency so that are displayed in the same big box.
    val groupedActions = sortedActions.groupBy{case (a,i) =>
      (flow.tagState.taggedActions(a.guid).tags, flow.tagState.taggedActions(a.guid).dependentOnTags)}

    //This auxiliary function helps to produce an easier to read box placing the capitalized action name nearly centred.
    def word(a : DataFlowAction[T, C]): String = {
      val str2: String = {
        if (a.actionName.toLowerCase == a.actionName) a.actionName.toUpperCase
        else a.actionName
      }
      if (str2.length < 11) "          " + str2 else str2
    }

    // r is a counter to enumerate correctly the big boxes.
    var r = flow.actions.length -1

    // Create the nodes to pass to WaimakGraph
    val theNodes: Seq[WaimakNode] = groupedActions.flatMap{case ((k1, k2), l) => //m => m match { //
      if  (k1.isEmpty && k2.isEmpty) {   //case ((List(), Set()), l) => {
        l.map{case (a, i) => WaimakNode(i,{
          word(a) + s"\nInputs:         ${a.inputLabels mkString ", " }"  + s"\nOutputs:      ${a.outputLabels mkString ", " }"
        }, "")}
      }
      else {   //case ((k1, k2), l) => {
        r += 1
        val str =
          if (k1.nonEmpty && k2.nonEmpty) s"Tags: ${k1 mkString ", " } \n DependentOnTags: ${k2 mkString ", " }"
          else if (k1.nonEmpty) s"Tags: ${k1 mkString ", " }"
          else s"TagDependency: ${k2 mkString ", " }"

        Seq(WaimakNode(r, str, "", l.map{case (a, i) => WaimakNode(i, {
          word(a) + s"\nInputs:         ${a.inputLabels mkString ", " }" + s"\nOutputs:      ${a.outputLabels mkString ", " }"
        }, "")}))
      }
    }.toSeq

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