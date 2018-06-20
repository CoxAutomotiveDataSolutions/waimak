package com.coxautodata.waimak.dataflow.spark

import java.io.FileNotFoundException

import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.metastore.HadoopDBConnector
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path, PathOperationException}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Introduces spark session into the data flows
  */
trait SparkDataFlow extends DataFlow[SparkFlowContext] with Logging {

  val spark: SparkSession

  override val flowContext: SparkFlowContext = SparkFlowContext(spark)

  /**
    * Folder into which the temp data will be saved before commit into the output storage: folders, RDBMs, Key Value tables.
    *
    * @return
    */
  def tempFolder: Option[Path]

  /**
    * Execution of the flow is lazy, but registration of the datasets as sql tables can only happen when data set is created.
    * With multiple threads consuming same table, registration of the data set as an sql table needs to happen in synchronised code.
    *
    * Labels that need to be registered as temp spark views before the execution starts. This is necessary if they
    * are to be reused by multiple parallel threads.
    *
    * @return
    */
  def sqlTables: Set[String]

  override def executed(executed: DataFlowAction[SparkFlowContext], outputs: Seq[Option[Any]]): DataFlow[SparkFlowContext] = {
    val res = super.executed(executed, outputs)
    // multiple sql actions might request same table, the simplest way of avoiding the race conditions of multiple actions
    // trying to register same dataset as an SQL table. To solve it, dataset will be reentered by the producing execution,
    // before multiple actions will try to use it
    executed.outputLabels.zip(outputs)
      .filter(p => p._2.isDefined && sqlTables.contains(p._1))
      .foreach(p => p._2.get.asInstanceOf[Dataset[_]].createOrReplaceTempView(p._1))
    res
  }

  override def prepareForExecution(): this.type = {
    val superPreparedFlow = super.prepareForExecution()
    superPreparedFlow.tempFolder match {
      case Some(p) => logInfo(s"Cleaning up temporary folder: ${p.toString}")
        superPreparedFlow.flowContext.fileSystem.delete(p, true)
        superPreparedFlow.flowContext.fileSystem.mkdirs(p)
      case None if superPreparedFlow.commitLabels.nonEmpty => throw new DataFlowException("Cannot add commit actions as no temporary folder is defined")
      case None => logInfo(s"Not cleaning up temporary folder as it is not defined")
    }
    // Add commit action
    val allLabels = (superPreparedFlow.inputs.keySet ++ superPreparedFlow.actions.flatMap(_.outputLabels)).toList
    if (superPreparedFlow.commitLabels.nonEmpty) superPreparedFlow.addAction(CommitAction(superPreparedFlow.commitLabels, superPreparedFlow.tempFolder.get, allLabels))
    else this
  }

  def addCommitLabel(label: String, definition: LabelCommitDefinition): SparkDataFlow

  val commitLabels: Map[String, LabelCommitDefinition]

}

case class LabelCommitDefinition(basePath: String, timestampFolder: Option[String] = None, partitions: Seq[String] = Seq.empty, connection: Option[HadoopDBConnector] = None)

private[spark] case class CommitAction(commitLabels: Map[String, LabelCommitDefinition], tempPath: Path, labelsToWaitFor: List[String]) extends SparkDataFlowAction with Logging {
  val inputLabels: List[String] = labelsToWaitFor
  val outputLabels: List[String] = List.empty

  override val requiresAllInputs = false

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): ActionResult = {

    // Create path objects
    val srcDestMap: Map[String, (Path, Path)] = commitLabels.map {
      e =>
        val tableName = e._1
        val destDef = e._2
        val srcPath = new Path(tempPath, tableName)
        val destPathBase = new Path(s"${destDef.basePath}/$tableName")
        val destPath = destDef.timestampFolder.map(new Path(destPathBase, _)).getOrElse(destPathBase)
        if (!flowContext.fileSystem.exists(srcPath)) throw new FileNotFoundException(s"Cannot commit table $tableName as " +
          s"the source path does not exist: ${srcPath.toUri.getPath}")
        if (flowContext.fileSystem.exists(destPath)) throw new FileAlreadyExistsException(s"Cannot commit table $tableName as " +
          s"the destination path already exists: ${destPath.toUri.getPath}")
        tableName -> (srcPath, destPath)
    }

    // Directory moving
    srcDestMap.foreach {
      e =>
        val label = e._1
        val srcPath = e._2._1
        val destPath = e._2._2
        if (!flowContext.fileSystem.exists(destPath.getParent)) {
          logInfo(s"Creating parent folder ${destPath.getParent.toUri.getPath} for label $label")
          val res = flowContext.fileSystem.mkdirs(destPath.getParent)
          if (!res) throw new PathOperationException(s"Could not create parent directory: ${destPath.getParent.toUri.getPath} for label $label")
        }
        val res = flowContext.fileSystem.rename(srcPath, destPath)
        if (!res) throw new PathOperationException(s"Could not move path ${srcPath.toUri.getPath} to ${destPath.toUri.getPath} for label $label")
    }

    // Table Commits
    srcDestMap.filterKeys(k => commitLabels(k).connection.isDefined).foreach {
      e =>
        val label = e._1
        val destPath = e._2._2
        val commit = commitLabels(label)
        commit.connection.get.updateTableParquetLocation(label, destPath.toUri.getPath, commit.partitions)
    }

    List.empty
  }
}
