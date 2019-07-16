package com.coxautodata.waimak.dataflow.spark

import java.io.FileNotFoundException

import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.metastore.HadoopDBConnector
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path, PathOperationException}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

/**
  * Introduces spark session into the data flows
  */
class SparkDataFlow(info: SparkDataFlowInfo) extends DataFlow with Logging {

  override val flowContext: SparkFlowContext = SparkFlowContext(spark)

  def spark: SparkSession = info.spark

  /**
    * Folder into which the temp data will be saved before commit into the output storage: folders, RDBMs, Key Value tables.
    *
    * @return
    */
  def tempFolder: Option[Path] = info.tempFolder

  /**
    * Execution of the flow is lazy, but registration of the datasets as sql tables can only happen when data set is created.
    * With multiple threads consuming same table, registration of the data set as an sql table needs to happen in synchronised code.
    *
    * Labels that need to be registered as temp spark views before the execution starts. This is necessary if they
    * are to be reused by multiple parallel threads.
    *
    * @return
    */
  def sqlTables: Set[String] = info.sqlTables

  override def schedulingMeta: SchedulingMeta = info.schedulingMeta

  override def schedulingMeta(sc: SchedulingMeta): SparkDataFlow.this.type = new SparkDataFlow(info.copy(schedulingMeta = sc)).asInstanceOf[this.type]

  /**
    * Inputs that were explicitly set or produced by previous actions, these are inputs for all following actions.
    * Inputs are preserved in the data flow state, even if they are no longer required by the remaining actions.
    * //TODO: explore the option of removing the inputs that are no longer required by remaining actions!!!
    *
    * @return
    */
  override def inputs: DataFlowEntities = info.inputs

  override def inputs(inp: DataFlowEntities): SparkDataFlow.this.type = new SparkDataFlow(info.copy(inputs = inp)).asInstanceOf[this.type]

  /**
    * Actions to execute, these will be scheduled when inputs become available. Executed actions must be removed from
    * the sate.
    *
    * @return
    */
  override def actions: Seq[DataFlowAction] = info.actions

  override def actions(acs: Seq[DataFlowAction]): SparkDataFlow.this.type = {
    val newSQLTables = sqlTables ++ acs.filter(_.getClass == classOf[SparkSimpleAction]).flatMap(a => a.asInstanceOf[SparkSimpleAction].sqlTables).toSet
    new SparkDataFlow(info.copy(actions = acs, sqlTables = newSQLTables)).asInstanceOf[this.type]
  }

  override def tagState: DataFlowTagState = info.tagState

  override def tagState(ts: DataFlowTagState): SparkDataFlow.this.type = new SparkDataFlow(info.copy(tagState = ts)).asInstanceOf[this.type]

  override def executed(executed: DataFlowAction, outputs: Seq[Option[Any]]): this.type = {
    val res = super.executed(executed, outputs)
    // multiple sql actions might request same table, the simplest way of avoiding the race conditions of multiple actions
    // trying to register same dataset as an SQL table. To solve it, dataset will be reentered by the producing execution,
    // before multiple actions will try to use it
    executed.outputLabels.zip(outputs)
      .filter(p => p._2.isDefined && sqlTables.contains(p._1))
      .foreach(p => p._2.get.asInstanceOf[Dataset[_]].createOrReplaceTempView(p._1))
    res
  }

  override def prepareForExecution(): Try[this.type] = {
    super.prepareForExecution()
      .map { superPreparedFlow =>
        superPreparedFlow.tempFolder match {
          case Some(p) => logInfo(s"Cleaning up temporary folder: ${p.toString}")
            superPreparedFlow.flowContext.fileSystem.delete(p, true)
            superPreparedFlow.flowContext.fileSystem.mkdirs(p)
          case None => logInfo(s"Not cleaning up temporary folder as it is not defined")
        }
        superPreparedFlow
      }
  }

  override def finaliseExecution(): Try[SparkDataFlow.this.type] = {
    import SparkDataFlow._
    super.finaliseExecution()
      .map {
        flow =>
          (flow.tempFolder, flow.flowContext.getBoolean(REMOVE_TEMP_AFTER_EXECUTION, REMOVE_TEMP_AFTER_EXECUTION_DEFAULT)) match {
            case (None, _) =>
              logInfo(s"Not cleaning up temporary folder after flow execution as it is not defined")
              flow
            case (Some(tmp), false) =>
              logInfo(s"Not cleaning up temporary folder [$tmp] after flow execution as [$REMOVE_TEMP_AFTER_EXECUTION] was false")
              flow
            case (Some(tmp), true) =>
              logInfo(s"Cleaning up temporary folder [$tmp] as [$REMOVE_TEMP_AFTER_EXECUTION] was true")
              flow.flowContext.fileSystem.delete(tmp, true)
              flow
          }
      }
  }

  override def executor: DataFlowExecutor = info.executor

  override def withExecutor(executor: DataFlowExecutor): this.type = new SparkDataFlow(info.copy(executor = executor)).asInstanceOf[this.type]

  override def setExtensionMetadata(newMetadata: Map[DataFlowExtension, DataFlowMetadataState]): SparkDataFlow.this.type = new SparkDataFlow(info.copy(extensionMetadata = newMetadata)).asInstanceOf[this.type]

  override def extensionMetadata: Map[DataFlowExtension, DataFlowMetadataState] = this.info.extensionMetadata
}

case class LabelCommitDefinition(basePath: String, timestampFolder: Option[String] = None, partitions: Seq[String] = Seq.empty, connection: Option[HadoopDBConnector] = None)

private[spark] case class CommitAction(commitLabels: Map[String, LabelCommitDefinition], tempPath: Path, labelsToWaitFor: List[String]) extends SparkDataFlowAction with Logging {
  val inputLabels: List[String] = labelsToWaitFor
  val outputLabels: List[String] = List.empty

  override val requiresAllInputs = false

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try {

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
    commitLabels.filter(_._2.connection.isDefined)
      .groupBy(_._2.connection.get)
      .mapValues(_.map {
        case (label, commitDefinition) =>
          commitDefinition.connection.get.updateTableParquetLocationDDLs(label, srcDestMap(label)._2.toUri.getPath, commitDefinition.partitions)
      }).foreach {
      case (connection, ddls) => connection.submitAtomicResultlessQueries(ddls.flatten.toSeq)
    }

    List.empty
  }
}

case class SparkDataFlowInfo(spark: SparkSession,
                             inputs: DataFlowEntities,
                             actions: Seq[DataFlowAction],
                             sqlTables: Set[String],
                             tempFolder: Option[Path],
                             schedulingMeta: SchedulingMeta,
                             commitLabels: Map[String, LabelCommitDefinition] = Map.empty,
                             tagState: DataFlowTagState = DataFlowTagState(Set.empty, Set.empty, Map.empty),
                             extensionMetadata: Map[DataFlowExtension, DataFlowMetadataState] = Map.empty,
                             executor: DataFlowExecutor = Waimak.sparkExecutor())

object SparkDataFlow {

  import DataFlow.dataFlowParamPrefix

  /**
    * Whether to clean up the temporary folder after a flow finishes execution.
    * Only cleans up if execution was successful.
    */
  val REMOVE_TEMP_AFTER_EXECUTION: String = s"$dataFlowParamPrefix.removeTempAfterExecution"
  val REMOVE_TEMP_AFTER_EXECUTION_DEFAULT: Boolean = true

  def empty(spark: SparkSession): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, DataFlowEntities.empty, Seq.empty, Set.empty, None, new SchedulingMeta()))

  def empty(spark: SparkSession, stagingFolder: Path): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, DataFlowEntities.empty, Seq.empty, Set.empty, Some(stagingFolder), new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Path, inputs: DataFlowEntities): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, inputs, Seq.empty, Set.empty, Some(stagingFolder), new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Path, inputs: DataFlowEntities, actions: Seq[DataFlowAction]): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, inputs, actions, Set.empty, Some(stagingFolder), new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction], sqlTables: Set[String]): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction], sqlTables: Set[String], commitLabels: Map[String, LabelCommitDefinition]): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta(), commitLabels))

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction], sqlTables: Set[String], commitLabels: Map[String, LabelCommitDefinition], tagState: DataFlowTagState): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta(), commitLabels, tagState))

}