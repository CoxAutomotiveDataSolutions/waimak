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

  override def commitMeta: CommitMeta = info.commitMeta

  override def commitMeta(cm: CommitMeta): SparkDataFlow.this.type = new SparkDataFlow(info.copy(commitMeta = cm)).asInstanceOf[this.type]

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

}

case class LabelCommitDefinition(labelName: String, basePath: String, timestampFolder: Option[String] = None, partitions: Seq[String] = Seq.empty, connection: Option[HadoopDBConnector] = None){
  private val destPathBase = new Path(s"$basePath/$labelName")
  val outputPath: Path = timestampFolder.map(new Path(destPathBase, _)).getOrElse(destPathBase)
}

case class SparkDataFlowInfo(spark: SparkSession,
                             inputs: DataFlowEntities,
                             actions: Seq[DataFlowAction],
                             sqlTables: Set[String],
                             tempFolder: Option[Path],
                             schedulingMeta: SchedulingMeta,
                             commitLabels: Map[String, LabelCommitDefinition] = Map.empty,
                             tagState: DataFlowTagState = DataFlowTagState(Set.empty, Set.empty, Map.empty),
                             commitMeta: CommitMeta = CommitMeta.empty)

object SparkDataFlow {

  def empty(spark: SparkSession): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, DataFlowEntities.empty, Seq.empty, Set.empty, None, new SchedulingMeta()))

  def empty(spark: SparkSession, stagingFolder: Path): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, DataFlowEntities.empty, Seq.empty, Set.empty, Some(stagingFolder), new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Path, inputs: DataFlowEntities): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, inputs, Seq.empty, Set.empty, Some(stagingFolder), new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Path, inputs: DataFlowEntities, actions: Seq[DataFlowAction]): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, inputs, actions, Set.empty, Some(stagingFolder), new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction], sqlTables: Set[String]): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction], sqlTables: Set[String], commitLabels: Map[String, LabelCommitDefinition]): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta(), commitLabels))

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction], sqlTables: Set[String], commitLabels: Map[String, LabelCommitDefinition], tagState: DataFlowTagState): SparkDataFlow = new SparkDataFlow(SparkDataFlowInfo(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta(), commitLabels, tagState))

}