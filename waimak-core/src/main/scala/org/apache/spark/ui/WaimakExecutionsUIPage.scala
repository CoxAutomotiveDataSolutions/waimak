package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

class WaimakExecutionsUIPage(parent: WaimakExecutionsUITab) extends WebUIPage("") {
  override def render(request: HttpServletRequest): Seq[Node] = {

    val dags: Iterable[(String, WaimakExecutionEvent)] = parent.listener.executions

    def renderRow(row: (String, WaimakExecutionEvent)): Seq[Node] = {
      val executionId = row._1
      val url = UIUtils.prependBaseUri(parent.basePath, s"flow?id=$executionId")
      val link = <tr>
        <td>
          <a href={url}>
            {executionId}
          </a>
        </td>
      </tr>
      Seq(link)
    }

    val table = UIUtils.listingTable(Seq("Execution GUID"), renderRow, dags, fixedWidth = true)

    val header =
      <h2>Dags:</h2>


    UIUtils.headerSparkPage("Waimak Executions", table, parent)
  }
}
