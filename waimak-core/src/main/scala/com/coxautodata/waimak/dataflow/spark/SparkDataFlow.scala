package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

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
}

case class SparkDataFlowInfo(spark: SparkSession,
                             inputs: DataFlowEntities,
                             actions: Seq[DataFlowAction],
                             sqlTables: Set[String],
                             tempFolder: Option[Path],
                             schedulingMeta: SchedulingMeta,
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
}