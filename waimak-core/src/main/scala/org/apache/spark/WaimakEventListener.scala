package org.apache.spark

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}

import scala.collection.mutable

class WaimakEventListener extends SparkListener {

  val executions = new mutable.HashMap[String, WaimakExecutionEvent]()

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e@WaimakExecutionEvent(id, _, _) => executions(id) = e
    case _ => Unit
  }
}