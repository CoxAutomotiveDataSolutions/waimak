package org.apache.spark.ui

import com.coxautodata.waimak.dataflow.spark.ui.WaimakGraph

import scala.xml.Node

object WaimakGraphRenderer {

  def createGraph(graph: WaimakGraph) : Seq[Node] = {
    planVisualization(graph)
  }


  private def planVisualization(graph: WaimakGraph): Seq[Node] = {
    val metadata = graph.allNodes.flatMap { node =>
      val nodeId = s"plan-meta-data-${node.id}"
      <div id={nodeId}></div>
    }

    <div>
      <div id="plan-viz-graph"></div>
      <div id="plan-viz-metadata" style="display:none">
        <div class="dot-file">
          {graph.makeDotFile}
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
