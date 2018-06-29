package org.apache.spark.ui

import org.apache.spark.{SparkContext, WaimakExecutionEvent, WaimakEventListener}

class WaimakExecutionsUITab(parent: SparkUI, val listener: WaimakEventListener) extends SparkUITab(parent, "waimak") {

  attachPage(new WaimakExecutionsUIPage(this))
  attachPage(new WaimakExecutionUIPage(this))

}

object WaimakExecutionsUITab {
  def apply(sparkContext: SparkContext): Unit = {
    val maybeUI = sparkContext.ui
    maybeUI.foreach(ui => {

      ui.getTabs.find {
        _.isInstanceOf[WaimakExecutionsUITab]
      } match {
        case Some(_) => Unit
        case None =>
          val listener = WaimakExecutionEvent.registerListener(sparkContext)
          ui.attachTab(new WaimakExecutionsUITab(ui, listener))
      }

    })

  }
}