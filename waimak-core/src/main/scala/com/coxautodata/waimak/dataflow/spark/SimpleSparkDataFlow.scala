package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

case class SimpleSparkDataFlowInfo(
                                    spark: SparkSession
                                    , inputs: DataFlowEntities
                                    , actions: Seq[DataFlowAction]
                                    , sqlTables: Set[String]
                                    , tempFolder: Option[Path]
                                    , schedulingMeta: SchedulingMeta
                                    , commitLabels: Map[String, LabelCommitDefinition] = Map.empty
                                    , tagState: DataFlowTagState = DataFlowTagState(Set.empty, Set.empty, Map.empty)
                                    , commitMeta: CommitMeta = CommitMeta.empty
                                  )
/**
  * Created by Alexei Perelighin on 22/12/17.
  */
class SimpleSparkDataFlow(info: SimpleSparkDataFlowInfo) extends SparkDataFlow with Logging {

//  override def addCommitLabel(label: String, definition: LabelCommitDefinition): SparkDataFlow = {
//    new SimpleSparkDataFlow(spark, inputs, actions, sqlTables, tempFolder, schedulingMeta, commitLabels + (label -> definition), tagState)
//  }

  override def spark: SparkSession = info.spark

  /**
    * Folder into which the temp data will be saved before commit into the output storage: folders, RDBMs, Key Value tables.
    *
    * @return
    */
  override def tempFolder: Option[Path] = info.tempFolder

  /**
    * Execution of the flow is lazy, but registration of the datasets as sql tables can only happen when data set is created.
    * With multiple threads consuming same table, registration of the data set as an sql table needs to happen in synchronised code.
    *
    * Labels that need to be registered as temp spark views before the execution starts. This is necessary if they
    * are to be reused by multiple parallel threads.
    *
    * @return
    */
  override def sqlTables: Set[String] = info.sqlTables

  override def addCommitLabel(label: String, definition: LabelCommitDefinition): SparkDataFlow = ???

  override val commitLabels: Map[String, LabelCommitDefinition] = info.commitLabels

  override def schedulingMeta: SchedulingMeta = info.schedulingMeta

  override def schedulingMeta(sc: SchedulingMeta): SimpleSparkDataFlow.this.type = new SimpleSparkDataFlow(info.copy(schedulingMeta = sc)).asInstanceOf[this.type]

  /**
    * Inputs that were explicitly set or produced by previous actions, these are inputs for all following actions.
    * Inputs are preserved in the data flow state, even if they are no longer required by the remaining actions.
    * //TODO: explore the option of removing the inputs that are no longer required by remaining actions!!!
    *
    * @return
    */
  override def inputs: DataFlowEntities = info.inputs

  override def inputs(inp: DataFlowEntities): SimpleSparkDataFlow.this.type = new SimpleSparkDataFlow(info.copy(inputs = inp)).asInstanceOf[this.type]

  /**
    * Actions to execute, these will be scheduled when inputs become available. Executed actions must be removed from
    * the sate.
    *
    * @return
    */
  override def actions: Seq[DataFlowAction] = info.actions

  override def actions(acs: Seq[DataFlowAction]): SimpleSparkDataFlow.this.type = {
    val newSQLTables = sqlTables ++ acs.filter(_.getClass == classOf[SparkSimpleAction]).flatMap(a => a.asInstanceOf[SparkSimpleAction].sqlTables).toSet
    new SimpleSparkDataFlow(info.copy(actions = acs, sqlTables = newSQLTables)).asInstanceOf[this.type]
  }

  override def tagState: DataFlowTagState = info.tagState

  override def tagState(ts: DataFlowTagState): SimpleSparkDataFlow.this.type = new SimpleSparkDataFlow(info.copy(tagState = ts)).asInstanceOf[this.type]

  override def commitMeta: CommitMeta = info.commitMeta

  override def commitMeta(cm: CommitMeta): SimpleSparkDataFlow.this.type = new SimpleSparkDataFlow(info.copy(commitMeta = cm)).asInstanceOf[this.type]

}

object SimpleSparkDataFlow {

  def empty(spark: SparkSession): SimpleSparkDataFlow = new SimpleSparkDataFlow(SimpleSparkDataFlowInfo(spark, DataFlowEntities.empty, Seq.empty, Set.empty, None, new SchedulingMeta()))

  def empty(spark: SparkSession, stagingFolder: Path): SimpleSparkDataFlow = new SimpleSparkDataFlow(SimpleSparkDataFlowInfo(spark, DataFlowEntities.empty, Seq.empty, Set.empty, Some(stagingFolder), new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Path, inputs: DataFlowEntities): SimpleSparkDataFlow = new SimpleSparkDataFlow(SimpleSparkDataFlowInfo(spark, inputs, Seq.empty, Set.empty, Some(stagingFolder), new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Path, inputs: DataFlowEntities, actions: Seq[DataFlowAction]): SimpleSparkDataFlow = new SimpleSparkDataFlow(SimpleSparkDataFlowInfo(spark, inputs, actions, Set.empty, Some(stagingFolder), new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction], sqlTables: Set[String]): SimpleSparkDataFlow = new SimpleSparkDataFlow(SimpleSparkDataFlowInfo(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta()))

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction], sqlTables: Set[String], commitLabels: Map[String, LabelCommitDefinition]): SimpleSparkDataFlow = new SimpleSparkDataFlow(SimpleSparkDataFlowInfo(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta(), commitLabels))

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction], sqlTables: Set[String], commitLabels: Map[String, LabelCommitDefinition], tagState: DataFlowTagState): SimpleSparkDataFlow = new SimpleSparkDataFlow(SimpleSparkDataFlowInfo(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta(), commitLabels, tagState))

}