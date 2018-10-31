package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.{CommitEntry, DataCommitter}
import org.apache.hadoop.fs.FileStatus
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.metastore.HadoopDBConnector

import scala.util.{Success, Try}

class ParquetDataCommitter(destFolder: String
                           , snapFolder: Option[String]
                           , clnUpStrategy: Option[CleanUpStrategy[FileStatus]]
                           , conn: Option[HadoopDBConnector])

      extends DataCommitter[SparkFlowContext, SparkDataFlow] {

  def snapshotFolder(folder: String) = new ParquetDataCommitter(destFolder, Some(folder), clnUpStrategy, conn)

  def cleanUpStrategy(strg: CleanUpStrategy[FileStatus]) = new ParquetDataCommitter(destFolder, snapFolder, Some(strg), conn)

  def connection(con: HadoopDBConnector) = new ParquetDataCommitter(destFolder, snapFolder, clnUpStrategy, Some(con))

  override protected[dataflow] def cacheToTempFlow(commitName: String, labels: Seq[CommitEntry], flow: SparkDataFlow): SparkDataFlow = {
    labels.foldLeft(flow) { (resFlow, labelCommitEntry) =>
      resFlow.cacheAsPartitionedParquet(labelCommitEntry.partitions, labelCommitEntry.repartition)(labelCommitEntry.label)
    }
  }

  override protected[dataflow] def moveToPermanentStorageFlow(commitName: String, labels: Seq[CommitEntry], flow: SparkDataFlow): SparkDataFlow = {

    ???
  }

  override protected[dataflow] def finish(commitName: String, labels: Seq[CommitEntry], flow: SparkDataFlow): SparkDataFlow = ???

  override protected[dataflow] def validate(): Try[Unit] = Success(Unit)

}

object ParquetDataCommitter {

  def apply(destinationFolder: String): ParquetDataCommitter = new ParquetDataCommitter(destinationFolder, None, None, None)

}