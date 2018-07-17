package com.coxautodata.waimak.dataflow.spark.ui

import com.coxautodata.waimak.dataflow.{DataFlow, DataFlowAction}


case class WaimakEdge(fromID: Int, toID: Int) {
  def makeDotEdge: String = s"""  $fromID->$toID;\n"""
}

//case class WaimakNode(id: Long, actionID: String, actionDesc: String, innerNodes: Seq[WaimakNode] = Seq.empty) {

case class WaimakNode(id: Long, actionName: String, inputLabels: List[String], outputLabels: List[String],
                      tagSet: Set[String] = Set(), tagDepsSet: Set[String] = Set(), innerNodes: Seq[WaimakNode] = Seq()) {

  // This auxiliary function helps to produce an easier to read box capitalizing the action name when appropriate.
  def capitalize(name: String): String = {
    if (name.toLowerCase == name) name.toUpperCase
    else name
  }

  // This function creates a small box displaying the action name, input labels and output labels.
  def makeDotNode: String = {
    val prettyDescription = capitalize(actionName) +
      s"\nInputs:         ${inputLabels mkString ", " }" +
      s"\nOutputs:      ${outputLabels mkString ", " }"
    if(innerNodes.nonEmpty) makeOuterDotNode
    else s"""  $id [label="$prettyDescription"];"""
  }

  // This function produces the string to be placed at the top right corner of the big boxes
  def clusterBoxString(s1: Set[String], s2: Set[String]): String =
    if (s1.nonEmpty && s2.nonEmpty) s"Tags: ${s1 mkString ", " } \n DependentOnTags: ${s2 mkString ", " }"
    else if (s1.nonEmpty) s"Tags: ${s1 mkString ", " }"
    else s"TagDependency: ${s2 mkString ", " }"

  // This function creates a cluster box displaying small boxes inside it.
  def makeOuterDotNode: String = {
    s"""
       |  subgraph cluster$id {
       |    label="${clusterBoxString(tagSet, tagDepsSet)}";
       |    ${innerNodes.map(_.makeDotNode).mkString("    \n")}
       |  }
     """.stripMargin
  }
}

case class WaimakGraph (nodes: Seq[WaimakNode], edges: Seq[WaimakEdge]) {

  def makeDotFile: String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    nodes.foreach(node => dotFile.append(node.makeDotNode + "\n"))
    edges.foreach(edge => dotFile.append(edge.makeDotEdge + "\n"))
    dotFile.append("}")
    dotFile.toString()
  }

  /**
    * All the WaimakNodes, including inner nodes
    */
  val allNodes: Seq[WaimakNode] = {
    nodes.flatMap {
      case outer@WaimakNode(_, _, _, _, _, _, innerNodes) if innerNodes.nonEmpty => innerNodes :+ outer
      case node => Seq(node)
    }
  }

}

object WaimakGraph {
  def apply[T, C](flow: DataFlow[T, C]): WaimakGraph = {

    // Let's first sort and enumerate the actions. The ones that do not have input dependencies should be at the top.
    val sortedActions = flow.actions.sortWith((a1, a2) => a1.inputLabels.length < a2.inputLabels.length).zipWithIndex

    // Group the actions by tags and tag's dependency so that they are displayed in the same big box.
    val groupedActions: Map[(Set[String], Set[String]), Seq[(DataFlowAction[T, C], Int)]] = sortedActions.groupBy{case (a,i) =>
      (flow.tagState.taggedActions(a.guid).tags, flow.tagState.taggedActions(a.guid).dependentOnTags)}

    // Create the nodes to pass to WaimakGraph
    // (cnt is a counter to enumerate correctly the big boxes).
    val (_, theNodes): (Int, Seq[WaimakNode]) = groupedActions.foldLeft(flow.actions.length -> Seq[WaimakNode]()){

      case ((cnt, seq), ((tagSet, tagDepsSet), l)) if tagSet.isEmpty && tagDepsSet.isEmpty =>
        (cnt, seq ++ l.map{case (a, i) => WaimakNode(i, a.actionName, a.inputLabels, a.outputLabels)})

      case ((cnt, seq), ((tagSet, tagDepsSet), l)) =>
        (cnt + 1, seq ++ Seq(WaimakNode(cnt, "", Nil, Nil, tagSet, tagDepsSet, l.map{case (a, i) =>
          WaimakNode(i, a.actionName, a.inputLabels, a.outputLabels)})))
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