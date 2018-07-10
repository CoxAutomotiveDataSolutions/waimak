package org.apache.spark

import com.coxautodata.waimak.dataflow.DataFlow
import org.apache.spark.sql.execution.ui.{WaimakEdge, WaimakNode}

case class WaimakGraph (nodes: Seq[WaimakNode], edges: Seq[WaimakEdge]) {}

object WaimakGraph {
  def apply[T, C](flow: DataFlow[T, C]): WaimakGraph = {

    // Let's first sort the actions. The ones that do not have input dependencies should be at the top.
    val sortedActions = flow.actions.sortWith((a1, a2) => a1.inputLabels.length < a2.inputLabels.length).zipWithIndex


    //val nodes: Seq[WaimakNode] = sortedActions.map(p => WaimakNode(p._2, s" Node ${p._1.actionName} ", ""))


    //val nodes: Seq[WaimakNode] = sortedActions.map{case (a,i) => WaimakNode(i, s" ${a.description} ", "")}

    //val nodes: Seq[WaimakNode] = sortedActions.map{
    //  case (a,i) => WaimakNode(i, s" ${a.inputLabels} \n" + s" ${a.actionName} \n" + s" ${a.outputLabels} ", "")}
    //val nodes: Seq[WaimakNode] = sortedActions.map{case (a,i) =>
    //  WaimakNode(i, s" Inputs: ${a.inputLabels} \n\n" + s" Action: ${a.actionName} \n\n" + s" Outputs: ${a.outputLabels} ", "")}

/*    val nodes: Seq[WaimakNode] = sortedActions.map{case (a,i) => WaimakNode(i,
          s"""
             |Inputs:       ${a.inputLabels}
             |Action:       ${a.actionName}
             |Tags:         ${flow.tagState.taggedActions(a.guid).tags}
             |TagDependency:${flow.tagState.taggedActions(a.guid).dependentOnTags}
             |Outputs:     ${a.outputLabels}""".stripMargin , "")}*/

    //val partitionedActions = sortedActions.partition()
    //val groupedActions = sortedActions.groupBy{case (a, i) => flow.tagState.taggedActions(a.guid).tags}
    //val groupedActions = flow.actions.groupBy{case a => flow.tagState.taggedActions(a.guid).tags}
    //val groupedActions = flow.actions.groupBy(a => (flow.tagState.taggedActions(a.guid).tags, flow.tagState.taggedActions(a.guid).dependentOnTags))

    val groupedActions = sortedActions.groupBy{case (a,i) => (flow.tagState.taggedActions(a.guid).tags, flow.tagState.taggedActions(a.guid).dependentOnTags)}
    println("grouped=", groupedActions)

    //val partitionedActions = sortedActions.partition(p => flow.tagState.taggedActions(p._1.guid).tags)
    //println("partitioned=", partitionedActions)
    //val partitionedActions = sortedActions.groupBy(flow.tagState.taggedActions(_.guid).tags)
    //println("partitionedActions", partitionedActions)

    var r = flow.actions.length -1

    val theNodes2: Seq[WaimakNode]  = groupedActions.map{case ((k1, k2), l) =>
      println("r=", r)
      r += 1
      //WaimakNode(r, s"Tags $k1 \n DependentOnTags: $k2", "", l.map{case (a, i) => WaimakNode(i,
      WaimakNode(r, s"Tags $r \n DependentOnTags: $k2", "", l.map{case (a, i) => WaimakNode(i,
        s"""
           |Inputs:       ${a.inputLabels}
           |Action:       ${a.actionName}
           |Tags:         ${flow.tagState.taggedActions(a.guid).tags}
           |TagDependency:${flow.tagState.taggedActions(a.guid).dependentOnTags}
           |Outputs:     ${a.outputLabels}""".stripMargin , "")})}.toSeq

/*
    val sortedActions2 = sortedActions.map{case (a, i) => (a, i+4)}

    val nodes2: Seq[WaimakNode] = sortedActions.map{case (a,i) => WaimakNode(i+4,
      s"""
         |Inputs:       ${a.inputLabels}
         |Action:       ${a.actionName}
         |Outputs:     ${a.outputLabels}""".stripMargin , "")}
*/
    //val allNodes: List[Seq[WaimakNode]] = nodes :: List(Seq())//nodes2)
    //val allNodes: List[Seq[WaimakNode]] = nodes :: List()//nodes2)

    val edges: Seq[WaimakEdge] =
      for {
        (aa, i) <- sortedActions //++ sortedActions2
        (ab, j) <- sortedActions //++ sortedActions2
        //if aa.guid != ab.guid &&
        if aa.outputLabels.toSet.intersect(ab.inputLabels.toSet).nonEmpty// && false
      } yield {
        //println("tag= ", aa)
        WaimakEdge(i, j)
      }

    val zipped = groupedActions.zipWithIndex
    println("zipped=", zipped)



    val outerEdges : Seq[WaimakEdge] =
      for {
        (((k11, k12), l), i) <- zipped.toSeq//groupedActions
        //(b, i) <- zipped.toSeq //groupedActions
        (((k21, k22), l), j) <- zipped//groupedActions
        //if i != j &&
        if k11.intersect(k22).nonEmpty
      } yield {WaimakEdge(j+flow.actions.length, i+flow.actions.length)}//.toSeq



          //val outerNode: Seq[WaimakNode] = (10 until 11).map(x => WaimakNode(x, "outerNode", "", nodes))


          /*

              val nodes1 = Seq(
                WaimakNode(0, "outer0", "", Seq(WaimakNode(1, "inner1", ""), WaimakNode(2, "inner2", "")))
                , WaimakNode(3, "outer3", "", Seq(WaimakNode(4, "inner4", "")))
                , WaimakNode(5, "outer5", "")
              )

              val edges1 = Seq(
                WaimakEdge(1, 4)
              )
          */



          //val node1: Seq[WaimakNode] = (1 until 2).map(u => WaimakNode(u, "TheInnerNode", ""))
          //val outerNode: Seq[WaimakNode] = (0 until 1).map(v => WaimakNode(v, "TheOuterNode", "", node1))
          //val theEdges: Seq[WaimakEdge] = for (x <- (1 until 2)) yield WaimakEdge(x, x)

          //val
          //val outerNodes: Seq[WaimakNode] = (0 until flow.actions.length).map(v => WaimakNode(v +flow.actions.length, s"OuterNode #$v", "", allNodes(v)))
          /*    val outerNodes: Seq[WaimakNode] = sortedActions
                .filter(p => flow.tagState.taggedActions(p._1.guid).tags.nonEmpty || flow.tagState.taggedActions(p._1.guid).dependentOnTags.nonEmpty)
                .map(pp => WaimakNode(v +flow.actions.length, s"OuterNode #$v", "", allNodes(v)))*/
          /*
              val cnt = flow.actions.length //-1

              val theNodes : Seq[WaimakNode] = sortedActions.map{case (a,i) =>
                if (flow.tagState.taggedActions(a.guid).tags.nonEmpty || flow.tagState.taggedActions(a.guid).dependentOnTags.nonEmpty)
                {println("passed 1")
                  println("i=", i, "cnt=", cnt)
                  //cnt += 1
                  WaimakNode(i +cnt, s"OuterNode #${i + cnt}", "", Seq(WaimakNode(i,
                    s"""
                       |Inputs:       ${a.inputLabels}
                       |Action:       ${a.actionName}
                       |Tags:         ${flow.tagState.taggedActions(a.guid).tags}
                       |TagDependency:${flow.tagState.taggedActions(a.guid).dependentOnTags}
                       |Outputs:     ${a.outputLabels}""".stripMargin , "")))}
                else
                {println("passed 2")
                  println("i=", i, "cnt=", cnt)
                  //cnt += 1
                  WaimakNode(i +cnt, s"OuterNode #${i + cnt}", "", Seq(WaimakNode(i,
                    s"""
                       |Inputs:       ${a.inputLabels}
                       |Action:       ${a.actionName}
                       |Tags:         ${flow.tagState.taggedActions(a.guid).tags}
                       |TagDependency:${flow.tagState.taggedActions(a.guid).dependentOnTags}
                       |Outputs:     ${a.outputLabels}""".stripMargin , "")))}
             }

              val outerNode: Seq[WaimakNode] = (0 until 1).map(v => WaimakNode(v +flow.actions.length, s"OuterNode #$v", "", allNodes(v)))
              */
          //val outerNode: Seq[WaimakNode] = (0 until 1).map(v => WaimakNode(v+8, s"OuterNode #$v", "", allNodes(v)))
          //val outerNode: Seq[WaimakNode] = (4 until 5).map(v => WaimakNode(v, "TheOuterNode", "", nodes))

          //WaimakGraph(nodes1, edges1)
          //WaimakGraph(nodes, edges)
          //WaimakGraph(outerNode, theEdges)
          //WaimakGraph(outerNode, edges)
            WaimakGraph(theNodes2, edges ++ outerEdges)
        }
      //}
}
