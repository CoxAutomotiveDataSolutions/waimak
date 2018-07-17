package org.apache.spark.sql.execution.ui

import org.apache.spark.ui.UIUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.xml.Node

//case class WaimakNode(id: Long, actionID: String, actionDesc: String)
case class WaimakNode(id: Long, actionID: String, actionDesc: String, innerNodes: Seq[WaimakNode] = Seq.empty)

case class WaimakEdge(fromID: Int, toID: Int)

object WaimakGraphRenderer {

  def createGraph(nodes: Seq[WaimakNode], edges: Seq[WaimakEdge]) : Seq[Node] = {

    val allNodes = nodes.map(n => {
      if (n.innerNodes.isEmpty)
        new SparkPlanGraphNode(n.id, n.actionID, n.actionDesc, Map.empty, Seq.empty)
      else {
        val buf: mutable.ArrayBuffer[SparkPlanGraphNode]= new ArrayBuffer[SparkPlanGraphNode]()
        n.innerNodes.map(m => new SparkPlanGraphNode(m.id, m.actionID, m.actionDesc, Map.empty, Seq.empty)).copyToBuffer(buf)
        new SparkPlanGraphCluster(n.id, n.actionID, n.actionDesc, buf, Seq.empty)
      }
    })

    planVisualization(Map.empty,
      new SparkPlanGraph(
        allNodes,
        edges.map(e => SparkPlanGraphEdge(e.fromID, e.toID))))
  }


  private def planVisualization(metrics: Map[Long, String], graph: SparkPlanGraph): Seq[Node] = {
    val metadata = graph.allNodes.flatMap { node =>
      val nodeId = s"plan-meta-data-${node.id}"
      <div id={nodeId}>{node.desc}</div>
    }

    <div>
      <div id="plan-viz-graph"></div>
      <div id="plan-viz-metadata" style="display:none">
        <div class="dot-file">
          {graph.makeDotFile(metrics)}
        </div>
        <div id="plan-viz-metadata-size">{graph.allNodes.size.toString}</div>
        {metadata}
      </div>
      {planVisualizationResources}
      <script>$(function() {{ renderPlanViz(); }})</script>
    </div>
  }

  private def planVisualizationResources: Seq[Node] = {
    // scalastyle:off
      <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/sql/spark-sql-viz.css")} type="text/css"/>
      <script src={UIUtils.prependBaseUri("/static/d3.min.js")}></script>
      <script src={UIUtils.prependBaseUri("/static/dagre-d3.min.js")}></script>
      <script src={UIUtils.prependBaseUri("/static/graphlib-dot.min.js")}></script>
      <script src={UIUtils.prependBaseUri("/static/sql/spark-sql-viz.js")}></script>
    // scalastyle:on
  }
}
