package com.coxautodata.waimak.dataflow.spark.ui

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.ui.WaimakExecutionEvent

import scala.collection.mutable

class WaimakEventListener extends SparkListener {

  val executions = new mutable.HashMap[String, WaimakExecutionEvent]()

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e@WaimakExecutionEvent(id, _) => executions(id) = e
    case _ => Unit
  }
}