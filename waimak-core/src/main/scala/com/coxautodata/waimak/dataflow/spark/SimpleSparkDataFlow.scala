package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
  * Created by Alexei Perelighin on 22/12/17.
  */
class SimpleSparkDataFlow(val spark: SparkSession
                          , val inputs: DataFlowEntities
                          , val actions: Seq[DataFlowAction[SparkFlowContext]]
                          , val sqlTables: Set[String]
                          , val tempFolder: Option[Path]
                          , val schedulingMeta: SchedulingMeta[SparkFlowContext]
                          , val commitLabels: Map[String, LabelCommitDefinition] = Map.empty
                          , val tagState: DataFlowTagState = DataFlowTagState(Set.empty, Set.empty, Map.empty)
                          , val commitMeta: CommitMeta[SparkFlowContext, DataFlow[SparkFlowContext]] = CommitMeta(Map.empty, Map.empty)
                         ) extends SparkDataFlow with Logging {

  override protected def createInstance(in: DataFlowEntities
                                        , ac: Seq[DataFlowAction[SparkFlowContext]]
                                        , tags: DataFlowTagState
                                        , schMeta: SchedulingMeta[SparkFlowContext]
                                        , commitMeta: CommitMeta[SparkFlowContext, DataFlow[SparkFlowContext]]): DataFlow[SparkFlowContext] = {
    // collect all labels that are inputs for SQL labels
    val newSQLTables = sqlTables ++ ac.filter(_.getClass == classOf[SparkSimpleAction]).flatMap(a => a.asInstanceOf[SparkSimpleAction].sqlTables).toSet
    new SimpleSparkDataFlow(spark, in, ac, newSQLTables, tempFolder, schMeta, commitLabels, tags, commitMeta)
  }

  override def addCommitLabel(label: String, definition: LabelCommitDefinition): SparkDataFlow = {
    new SimpleSparkDataFlow(spark, inputs, actions, sqlTables, tempFolder, schedulingMeta, commitLabels + (label -> definition), tagState)
  }
}

object SimpleSparkDataFlow {

  def empty(spark: SparkSession): SimpleSparkDataFlow = new SimpleSparkDataFlow(spark, DataFlowEntities.empty, Seq.empty, Set.empty, None, new SchedulingMeta[SparkFlowContext]())

  def empty(spark: SparkSession, stagingFolder: Path): SimpleSparkDataFlow = new SimpleSparkDataFlow(spark, DataFlowEntities.empty, Seq.empty, Set.empty, Some(stagingFolder), new SchedulingMeta[SparkFlowContext]())

  def apply(spark: SparkSession, stagingFolder: Path, inputs: DataFlowEntities): SimpleSparkDataFlow = new SimpleSparkDataFlow(spark, inputs, Seq.empty, Set.empty, Some(stagingFolder), new SchedulingMeta[SparkFlowContext]())

  def apply(spark: SparkSession, stagingFolder: Path, inputs: DataFlowEntities, actions: Seq[DataFlowAction[SparkFlowContext]]): SimpleSparkDataFlow = new SimpleSparkDataFlow(spark, inputs, actions, Set.empty, Some(stagingFolder), new SchedulingMeta[SparkFlowContext]())

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction[SparkFlowContext]], sqlTables: Set[String]): SimpleSparkDataFlow = new SimpleSparkDataFlow(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta[SparkFlowContext]())

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction[SparkFlowContext]], sqlTables: Set[String], commitLabels: Map[String, LabelCommitDefinition]): SimpleSparkDataFlow = new SimpleSparkDataFlow(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta[SparkFlowContext](), commitLabels)

  def apply(spark: SparkSession, stagingFolder: Option[Path], inputs: DataFlowEntities, actions: Seq[DataFlowAction[SparkFlowContext]], sqlTables: Set[String], commitLabels: Map[String, LabelCommitDefinition], tagState: DataFlowTagState): SimpleSparkDataFlow = new SimpleSparkDataFlow(spark, inputs, actions, sqlTables, stagingFolder, new SchedulingMeta[SparkFlowContext](), commitLabels, tagState)

}