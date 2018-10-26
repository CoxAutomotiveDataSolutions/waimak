package com.coxautodata.waimak.dataflow

abstract class DataCommitter[C, F <: DataFlow[C]] {

  protected def cacheToTempFlow(commitName: String, labels: Seq[String], flow: F): F

  protected def moveToPermanentStorageFlow(commitName: String, labels: Seq[String], flow: F): F

  protected def finish(commitName: String, labels: Seq[String], flow: F): F

  def build(commitName: String, labels: Seq[String], flow: F): F = {
    flow.tag(commitName) {
      cacheToTempFlow(commitName, labels, _)
    }.tagDependency(commitName) {
      _.tag(commitName + "_AFTER_COMMIT") {
        moveToPermanentStorageFlow(commitName, labels, _)
      }.tagDependency(commitName + "_AFTER_COMMIT") {
        finish(commitName, labels, _)
      }
    }
  }

}
