package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest
import org.apache.commons.lang3.StringEscapeUtils


import scala.xml.Node

class WaimakExecutionUIPage(parent: WaimakExecutionsUITab) extends WebUIPage("flow") {
  override def render(request: HttpServletRequest): Seq[Node] = {

    val parameter: String = request.getParameter("id")

    val parameterId: String = {if (parameter == null) {
      null
    } else {
      // Remove new lines and single quotes, followed by escaping HTML version 4.0
      StringEscapeUtils.escapeHtml4(
        raw"(?i)(\r\n|\n|\r|%0D%0A|%0A|%0D|'|%27)".r.replaceAllIn(parameter, ""))
    }}

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

    val content =
      <div>content for id
        {parameterId}
        is
        {flowContent}
      </div>

    UIUtils.headerSparkPage(s"Details for flow execution: $parameterId", graph, parent) //, showVisualization = true)
  }
}
