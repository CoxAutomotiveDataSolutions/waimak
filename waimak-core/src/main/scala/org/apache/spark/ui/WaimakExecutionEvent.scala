package org.apache.spark.ui

import com.coxautodata.waimak.dataflow.spark.ui.{WaimakEventListener, WaimakGraph}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListenerEvent

case class WaimakExecutionEvent(executionId: String, actionDescriptions: Seq[String], flowGraph: WaimakGraph) extends SparkListenerEvent

object WaimakExecutionEvent {

  def addEvent(sc: SparkContext, event: WaimakExecutionEvent): Unit = {
    sc.listenerBus.post(event)
  }

  def registerListener(sc: SparkContext): WaimakEventListener = {
    val listener = new WaimakEventListener
    //sc.listenerBus.addListener(listener)
    sc.addSparkListener(listener)
    listener
  }

}