package com.coxautodata.waimak.dataflow

import scala.util.Try

abstract class DataCommitter[C, F <: DataFlow[C]] {

  protected[dataflow] def validate(): Try[Unit]

  protected[dataflow] def cacheToTempFlow(commitName: String, labels: Seq[CommitEntry], flow: F): F

  protected[dataflow] def moveToPermanentStorageFlow(commitName: String, labels: Seq[CommitEntry], flow: F): F

  protected[dataflow] def finish(commitName: String, labels: Seq[CommitEntry], flow: F): F

}
