package com.coxautodata.waimak.dataflow.spark

import java.util.UUID

import com.coxautodata.waimak.dataflow.{ActionResult, _}
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.metastore.HadoopDBConnector
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.{FileStatus, Path}

import scala.util.Try

/**
  * Adds actions necessary to commit labels as parquet parquet, supports snapshot folders and interaction with a DB
  * connector.
  *
  * Created by Alexei Perelighin on 2018/11/05
  *
  * @param outputBaseFolder  folder under which final labels will store its data. Ex: baseFolder/label_1/
  * @param snapshotFolder    optional name of the snapshot folder that will be used by all of the labels committed via this committer.
  *                          It needs to be a full name and must not be the same as in any of the previous snapshots for any of
  *                          the commit managed labels.
  *                          Ex:
  *                          baseFolder/label_1/snapshot_folder=20181128
  *                          baseFolder/label_1/snapshot_folder=20181129
  *                          baseFolder/label_2/snapshot_folder=20181128
  *                          baseFolder/label_2/snapshot_folder=20181129
  * @param cleanupStrategy   optional function that takes the list of available snapshots and returns list of snapshots to remove
  * @param hadoopDBConnector optional connector to the DB.
  */
case class ParquetDataCommitter(outputBaseFolder: String,
                                snapshotFolder: Option[String] = None,
                                cleanupStrategy: Option[CleanUpStrategy[FileStatus]] = None,
                                hadoopDBConnector: Option[HadoopDBConnector] = None) extends DataCommitter[SparkDataFlow] with Logging {

  /**
    * Set a snapshot folder for this Parquet Committer
    */
  def withSnapshotFolder(folder: String): ParquetDataCommitter = this.copy(snapshotFolder = Some(folder))

  /**
    * Set a cleanup strategy for this Parquet Committer
    */
  def withCleanupStrategy(strategy: CleanUpStrategy[FileStatus]): ParquetDataCommitter = this.copy(cleanupStrategy = Some(strategy))

  /**
    * Configures a default implementation of a cleanup strategy based on dates encoded into snapshot folder name.
    *
    */
  def withDateBasedSnapshotCleanup(folderPrefix: String, dateFormat: String, numberOfFoldersToKeep: Int): ParquetDataCommitter = {
    import ParquetDataCommitter._
    withCleanupStrategy(dateBasedSnapshotCleanupStrategy(folderPrefix, dateFormat, numberOfFoldersToKeep)(fileStatusToName))
  }

  /**
    * Sets new DB connector
    */
  def withHadoopDBConnector(con: HadoopDBConnector): ParquetDataCommitter = this.copy(hadoopDBConnector = Some(con))

  private def commitTempPath(commitName: String, commitUUID: UUID, tempRoot: Option[Path]): Path = {
    tempRoot
      .map(new Path(_, commitUUID.toString))
      .getOrElse(throw new DataFlowException(s"Cannot add ParquetDataCommitter for commit name [$commitName] as no flow temporary folder has been given"))
  }

  override protected[dataflow] def stageToTempFlow(commitName: String, commitUUID: UUID, labels: Seq[CommitEntry], flow: SparkDataFlow): SparkDataFlow = {
    val sparkFlow = flow.asInstanceOf[SparkDataFlow]
    val commitTempBase = commitTempPath(commitName, commitUUID, sparkFlow.tempFolder).toString
    labels.foldLeft(sparkFlow) { (resFlow, labelCommitEntry) =>
      logInfo(s"Commit: $commitName, label: ${labelCommitEntry.label}, writing parquet into temp.")
      resFlow
        .map {
          case f if labelCommitEntry.cache => CacheAsParquetExtension.addCacheAsParquet(f, labelCommitEntry.label, labelCommitEntry.partitions, labelCommitEntry.repartition)
          case f => f
        }
        .writeRepartitionedPartitionedParquet(commitTempBase, labelCommitEntry.partitions, labelCommitEntry.repartition)(labelCommitEntry.label)
    }
  }

  override protected[dataflow] def moveToPermanentStorageFlow(commitName: String, commitUUID: UUID, labels: Seq[CommitEntry], flow: SparkDataFlow): SparkDataFlow = {
    val sparkFlow = flow.asInstanceOf[SparkDataFlow]
    val commitTempBase = commitTempPath(commitName, commitUUID, sparkFlow.tempFolder)
    val commitLabels = labels.map(ce => (ce.label, LabelCommitDefinition(outputBaseFolder, snapshotFolder, ce.partitions.flatMap(_.left.toOption).getOrElse(Seq.empty), hadoopDBConnector))).toMap
    sparkFlow.addAction(CommitAction(commitLabels, commitTempBase))
  }

  override protected[dataflow] def finish(commitName: String, commitUUID: UUID, labels: Seq[CommitEntry], flow: SparkDataFlow): SparkDataFlow = cleanupStrategy.fold(flow) { strategy =>
    labels.foldLeft(flow) { (resFlow, labelCommitEntry) =>
      resFlow.addAction(FSCleanUp(outputBaseFolder, strategy, List(labelCommitEntry.label)))
    }
  }

  /**
    * Validates that:
    * 1) data flow is a decedent of the SparkDataFlow
    * 2) data flow has temp folder
    * 3) no committed label has an existing snapshot folder same as new one
    * 4) cleanup can only take place when snapshot folder is defined
    *
    * @param flow data flow to validate
    * @param commitName
    * @param entries
    * @return
    */
  override protected[dataflow] def validate(flow: SparkDataFlow, commitName: String, entries: Seq[CommitEntry]): Try[Unit] = {
    Try {
      if (!classOf[SparkDataFlow].isAssignableFrom(flow.getClass)) throw new DataFlowException(s"""ParquetDataCommitter [$commitName] can only work with data flows derived from ${classOf[SparkDataFlow].getName}""")
      val sparkDataFlow = flow.asInstanceOf[SparkDataFlow]
      if (sparkDataFlow.tempFolder.isEmpty) throw new DataFlowException(s"ParquetDataCommitter [$commitName], temp folder is not defined")
      snapshotFolder
        .map(snp => s"$outputBaseFolder/*/$snp")
        .map(pattern => sparkDataFlow.flowContext.fileSystem.globStatus(new Path(pattern)))
        .map { matched =>
          val labels = entries.map(_.label).toSet
          matched.map(_.getPath.getParent.getName).filter(labels.contains).sorted
        }.filter(_.nonEmpty)
        .foreach(existing => throw new DataFlowException(s"ParquetDataCommitter [$commitName], snapshot folder [${snapshotFolder.get}] is already present for labels: ${existing.mkString("[", ", ", "]")}"))
      (snapshotFolder, cleanupStrategy) match {
        case (None, Some(_)) => throw new DataFlowException(s"ParquetDataCommitter [$commitName], cleanup will only work when snapshot folder is defined")
        case _ =>
      }
      Unit
    }
  }

}

object ParquetDataCommitter {

  /**
    * Implements a cleanup strategy that sorts input list of snapshots by timestamp extracted from the folder names
    * and ensures that there is at most numberOfFoldersToKeep of folders with latest timestamp left.
    * Folder names must be of same pattern as hive partition columns. Example: COLUMNNAME=TIMESTAMP
    *
    * @param columnName            column name part of the snapshot folder
    * @param timeStampFormat       Java format of the TIMESTAMP. Ex: yyyyMMddHHmmss
    * @param numberOfFoldersToKeep maximum number of snapshots to keep
    * @param getName               returns name of the snapshot
    * @tparam T type that identifies snapshot
    * @return configured cleanup strategy that returns list of snapshots to remove
    */
  def dateBasedSnapshotCleanupStrategy[T](columnName: String, timeStampFormat: String, numberOfFoldersToKeep: Int)(getName: T => String): CleanUpStrategy[T] = {
    import java.time._
    import java.time.format._

    val folderPrefix = columnName + "="
    val formatter = DateTimeFormatter.ofPattern(timeStampFormat)

    def res(table: TableName, snapshotFolders: InputSnapshots[T]): SnapshotsToDelete[T] = {
      snapshotFolders
        .filter(getName(_).startsWith(folderPrefix))
        .map(snapFolder => (LocalDateTime.parse(getName(snapFolder).substring(folderPrefix.length), formatter), snapFolder))
        .sortWith((d1, d2) => d1._1.isAfter(d2._1))
        .map(_._2)
        .drop(numberOfFoldersToKeep)
    }

    res
  }

  def fileStatusToName: FileStatus => String = _.getPath.getName

}

/**
  * Action that deletes snapshots based on the cleanup strategy. It can cleanup one or more labels.
  *
  * @param baseFolder root folder that contains label folders
  * @param toRemove   returns list of snapshot/folder to remove
  */
case class FSCleanUp(baseFolder: String
                     , toRemove: CleanUpStrategy[FileStatus]
                     , labelsToClean: Seq[String]
                    ) extends SparkDataFlowAction with Logging {
  override val actionName: String = "FSCleanUp"

  /**
    * Perform the action
    *
    * @param inputs      the [[DataFlowEntities]] corresponding to the [[inputLabels]]
    * @param flowContext context of the flow in which this action runs
    * @return the action outputs (these must be declared in the same order as their labels in [[outputLabels]])
    */
  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = {
    val basePath = new Path(baseFolder)
    val foldersToRemove = labelsToClean
      .map(l => (l, new Path(basePath, l)))
      .filter(lp => flowContext.fileSystem.exists(lp._2))
      .map(lp => (lp._1, flowContext.fileSystem.listStatus(lp._2).filter(_.isDirectory)))
      .map(labelSnapshots => (labelSnapshots._1, toRemove(labelSnapshots._1, labelSnapshots._2)))

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
  override val inputLabels: List[String] = List.empty
}