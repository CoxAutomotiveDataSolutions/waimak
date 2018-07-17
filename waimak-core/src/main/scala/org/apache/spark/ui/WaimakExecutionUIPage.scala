package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest
import org.apache.spark.sql.execution.ui._

//import org.apache.spark.WaimakGraph

import scala.xml.Node

class WaimakExecutionUIPage(parent: WaimakExecutionsUITab) extends WebUIPage("flow") {
  override def render(request: HttpServletRequest): Seq[Node] = {

    val parameterId = UIUtils.stripXSS(request.getParameter("id"))
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")
    require(parent.listener.executions.get(parameterId).isDefined, "id not a valid execution")

    val flowContent: WaimakExecutionEvent = parent.listener.executions(parameterId)

    def renderRow(row: String): Seq[Node] = {
      val actionDescription = row
      val line = <tr>
        <td>
          {actionDescription}
        </td>
      </tr>
      Seq(line)
    }

    val graph = WaimakGraphRenderer.createGraph(flowContent.flowGraph)

    val table = UIUtils.listingTable(Seq("Action Description"), renderRow, flowContent.actionDescriptions, fixedWidth = true)

    val content =
      <div>content for id
        {parameterId}
        is
        {flowContent}
      </div>

    UIUtils.headerSparkPage(s"Details for flow execution: $parameterId", table ++ graph, parent) //, showVisualization = true)
  }
}
