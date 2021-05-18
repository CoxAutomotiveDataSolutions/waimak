package com.coxautodata.waimak.dataflow.spark

import java.io.FileNotFoundException
import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.metastore.HadoopDBConnector
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path, PathOperationException}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  * Introduces spark session into the data flows
  */
class SparkDataFlow(info: SparkDataFlowInfo) extends DataFlow[SparkDataFlow] with Logging {

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

  def commitLabels: Map[String, LabelCommitDefinition] = info.commitLabels

  def extensionMetadata: Set[DataFlowMetadataExtension[SparkDataFlow]] = info.extensionMetadata

  override def schedulingMeta: SchedulingMeta = info.schedulingMeta

  override def schedulingMeta(sc: SchedulingMeta): SparkDataFlow = new SparkDataFlow(info.copy(schedulingMeta = sc))

  /**
    * Inputs that were explicitly set or produced by previous actions, these are inputs for all following actions.
    * Inputs are preserved in the data flow state, even if they are no longer required by the remaining actions.
    * //TODO: explore the option of removing the inputs that are no longer required by remaining actions!!!
    *
    * @return
    */
  override def inputs: DataFlowEntities = info.inputs

  override def inputs(inp: DataFlowEntities): SparkDataFlow = new SparkDataFlow(info.copy(inputs = inp))

  /**
    * Actions to execute, these will be scheduled when inputs become available. Executed actions must be removed from
    * the sate.
    *
    * @return
    */
  override def actions: Seq[DataFlowAction] = info.actions

  override def actions(acs: Seq[DataFlowAction]): SparkDataFlow = {
    val newSQLTables = sqlTables ++ acs.collect { case s: SparkSimpleAction => s.sqlTables }.flatten.toSet
    new SparkDataFlow(info.copy(actions = acs, sqlTables = newSQLTables))
  }

  override def tagState: DataFlowTagState = info.tagState

  override def tagState(ts: DataFlowTagState): SparkDataFlow = new SparkDataFlow(info.copy(tagState = ts))


  override def executed(executed: DataFlowAction, outputs: Seq[Option[Any]]): SparkDataFlow = {

    val res = super.executed(executed, outputs)
    // multiple sql actions might request same table, the simplest way of avoiding the race conditions of multiple actions
    // trying to register same dataset as an SQL table. To solve it, dataset will be reentered by the producing execution,
    // before multiple actions will try to use it
    executed.outputLabels.zip(outputs)
      .filter(p => p._2.isDefined && sqlTables.contains(p._1))
      .foreach(p => p._2.get.asInstanceOf[Dataset[_]].createOrReplaceTempView(p._1))
    res
  }

  override def prepareForExecution(): Try[SparkDataFlow] = {
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

  // This includes a check that we have a valid DAG, which is useful for after doing a combine
  def checkFlowState(): Try[SparkDataFlow] = super.isValidFlowDAG

  override def finaliseExecution(): Try[SparkDataFlow] = {
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

  override def metadataExtensions: Set[DataFlowMetadataExtension[SparkDataFlow]] = this.info.extensionMetadata

  override def withExecutor(executor: DataFlowExecutor): SparkDataFlow = new SparkDataFlow(info.copy(executor = executor))

  override def setMetadataExtensions(extensions: Set[DataFlowMetadataExtension[SparkDataFlow]]): SparkDataFlow = new SparkDataFlow(info.copy(extensionMetadata = extensions))

  // Try to merge flows for parallel execution, e.g. just smash all their internal structures together
  def ++(that: SparkDataFlow): SparkDataFlow = {
    new SparkDataFlow(
      info.copy(
        inputs = this.inputs ++ that.inputs,
        actions = this.actions ++ that.actions,
        sqlTables = this.sqlTables.union(that.sqlTables),
        tempFolder = this.tempFolder.orElse(that.tempFolder),
        commitLabels = info.commitLabels ++ that.commitLabels,
        tagState = this.tagState ++ that.tagState,
        extensionMetadata = info.extensionMetadata.union(that.extensionMetadata)
      )
    ).checkFlowState() match {
      case Success(sdf) => sdf
      case Failure(exception) => throw exception
    }
  }

}

case class LabelCommitDefinition(basePath: String, timestampFolder: Option[String] = None, partitions: Seq[String] = Seq.empty, connection: Option[HadoopDBConnector] = None)

private[spark] case class CommitAction(commitLabels: Map[String, LabelCommitDefinition], tempPath: Path) extends SparkDataFlowAction with Logging {
  val inputLabels: List[String] = List.empty
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
                             extensionMetadata: Set[DataFlowMetadataExtension[SparkDataFlow]] = Set.empty,
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

  def combine(a: SparkDataFlow, b: SparkDataFlow): SparkDataFlow = a ++ b

  def combine(a: SparkDataFlow, flows: SparkDataFlow*): SparkDataFlow =
    a.foldLeftOver(flows)((a, b) => a ++ b)

  def combine(a: SparkDataFlow, b: SparkDataFlow, flows: SparkDataFlow*): SparkDataFlow =
    (a ++ b).foldLeftOver(flows)((a, b) => a ++ b)

  def combine(flows: SparkDataFlow*): SparkDataFlow = combineAll(flows)

  def combineAll(flows: Seq[SparkDataFlow]): SparkDataFlow =
    flows.reduce(_ ++ _)
}