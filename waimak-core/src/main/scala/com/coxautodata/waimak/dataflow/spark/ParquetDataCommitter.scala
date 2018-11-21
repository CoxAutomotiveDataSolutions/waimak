package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.{ActionResult, _}
import org.apache.hadoop.fs.{FileStatus, Path}
import com.coxautodata.waimak.dataflow.spark.SparkActions._
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.metastore.HadoopDBConnector
import org.apache.hadoop.fs.permission.FsAction

import scala.util.{Failure, Success, Try}

/**
  * Created by Alexei Perelighin on 2018/11/05
  *
  * @param destFolder
  * @param snapFolder
  * @param clnUpStrategy
  * @param conn
  */
class ParquetDataCommitter(destFolder: String
                           , snapFolder: Option[String]
                           , clnUpStrategy: Option[CleanUpStrategy[FileStatus]]
                           , conn: Option[HadoopDBConnector])
      extends DataCommitter {

  def snapshotFolder(folder: String) = new ParquetDataCommitter(destFolder, Some(folder), clnUpStrategy, conn)

  def cleanUpStrategy(strg: CleanUpStrategy[FileStatus]) = new ParquetDataCommitter(destFolder, snapFolder, Some(strg), conn)

  def connection(con: HadoopDBConnector) = new ParquetDataCommitter(destFolder, snapFolder, clnUpStrategy, Some(con))

  override protected[dataflow] def cacheToTempFlow(commitName: String, labels: Seq[CommitEntry], flow: DataFlow): DataFlow = {
    val sparkFlow = flow.asInstanceOf[SparkDataFlow]
    labels.foldLeft(flow) { (resFlow, labelCommitEntry) =>
      sparkFlow.cacheAsPartitionedParquet(labelCommitEntry.partitions, labelCommitEntry.repartition)(labelCommitEntry.label)
    }
  }

  override protected[dataflow] def moveToPermanentStorageFlow(commitName: String, labels: Seq[CommitEntry], flow: DataFlow): DataFlow = {
    val sparkFlow = flow.asInstanceOf[SparkDataFlow]
    val commitLabels = labels.map(ce => (ce.label, LabelCommitDefinition(destFolder, snapFolder, ce.partitions, conn))).toMap
    sparkFlow.addAction(CommitAction(commitLabels, sparkFlow.tempFolder.get, labels.map(_.label).toList))
  }

  override protected[dataflow] def finish(commitName: String, labels: Seq[CommitEntry], flow: DataFlow): DataFlow = clnUpStrategy.fold(flow) { strategy =>
    labels.foldLeft(flow) { (resFlow, labelCommitEntry) =>
      resFlow.addAction(new FSCleanUp(destFolder, strategy, List(labelCommitEntry.label)))
    }
  }

  override protected[dataflow] def validate(flow: DataFlow): Try[Unit] = {
    Try {
      if (!classOf[SparkDataFlow].isAssignableFrom(flow.getClass)) throw new DataFlowException(s"""ParquetDataCommitter can only work with data flows derived from ${classOf[SparkDataFlow].getName}""")
      val sparkDataFlow = flow.asInstanceOf[SparkDataFlow]
      if (!sparkDataFlow.tempFolder.isDefined) throw new DataFlowException(s"""ParquetDataCommitter, temp folder is not defined""")
      Unit
    }
  }

}

object ParquetDataCommitter {

  def apply(destinationFolder: String): ParquetDataCommitter = new ParquetDataCommitter(destinationFolder, None, None, None)

}

/**
  *
  * @param destFolder
  * @param clnUpStrategy  returns list of folders that can be removed
  * @param inputLabels
  * @param outputLabels
  * @param actionName
  */
class FSCleanUp(destFolder: String
                , clnUpStrategy: CleanUpStrategy[FileStatus]
                , val inputLabels: List[String]
                , override val actionName: String = "FSCleanUp") extends SparkDataFlowAction with Logging {

  /**
    * Perform the action
    *
    * @param inputs      the [[DataFlowEntities]] corresponding to the [[inputLabels]]
    * @param flowContext context of the flow in which this action runs
    * @return the action outputs (these must be declared in the same order as their labels in [[outputLabels]])
    */
  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = {
    val baseFolder = new Path(destFolder)
    val foldersToRemove = inputLabels
      .map(l => (l, new Path(baseFolder, l)))
      .map(lp => (lp._1, flowContext.fileSystem.listStatus(lp._2).filter(_.isDirectory)))
      .map(labelSnapshots => (labelSnapshots._1, clnUpStrategy(labelSnapshots._1, labelSnapshots._2)))

    Try {
      foldersToRemove.foreach { toRemove =>
        val nonWritable = toRemove._2.filter(!_.getPermission.getUserAction.implies(FsAction.WRITE))
        if (nonWritable.nonEmpty) throw new DataFlowException(nonWritable.map(f => s"Label: ${toRemove._1}. Do not have permissions to remove ${f.getPath.toString}.").mkString("\n"))
      }
      foldersToRemove
    }.map { ftr =>
      ftr.foreach { toRemove =>
        if (toRemove._2.isEmpty) {
          logInfo(s"Nothing to clean up for label ${toRemove._1}")
        } else {
          toRemove._2.map { folderToRemove =>
            logInfo(s"Label: ${toRemove._1}. Removing folder: ${folderToRemove.getPath.toString}")
            flowContext.fileSystem.delete(folderToRemove.getPath, true)
          }
        }
      }
      Seq.empty
    }
  }

  /**
    * The unique identifiers for the outputs to this action
    */
  override val outputLabels: List[String] = List.empty

}