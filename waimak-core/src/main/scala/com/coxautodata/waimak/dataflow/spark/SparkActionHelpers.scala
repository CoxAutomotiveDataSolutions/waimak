package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.{ActionResult, DataFlowEntities, DataFlowException}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Dataset, SaveMode, SparkSession}

import scala.util.Try

object SparkActionHelpers {

  /**
    * Base function for all write operation, in most of the cases users should use more specialised one.
    * This one is used by other builders.
    *
    * @param dataFlow - flow to which to add the write action
    * @param label    - label whose data set will be written out
    * @param pre      - dataset transformation function
    * @param dfr      - dataframe writer function
    * @return
    */
  def writeBase(dataFlow: SparkDataFlow, label: String)(pre: Dataset[_] => Dataset[_])(dfr: DataFrameWriter[_] => Unit): SparkDataFlow = {
    def run(m: DataFlowEntities): ActionResult = {
      dfr(pre(m.get[Dataset[_]](label)).write)
      Seq.empty
    }

    dataFlow.addAction(new SimpleAction(List(label), List.empty, run, "write"))
  }

  def applyWriterOptions(options: Map[String, String]): DataFrameWriter[_] => DataFrameWriter[_] = {
    writer => options.foldLeft(writer)((z, c) => z.option(c._1, c._2))
  }

  def applyRepartition(partitionCols: Seq[String], repartition: Boolean): Dataset[_] => Dataset[_] = {
    ds => if (repartition && partitionCols.nonEmpty) ds.repartition(partitionCols.map(ds(_)): _*) else ds
  }

  def applyRepartition(repartition: Int): Dataset[_] => Dataset[_] = {
    ds => ds.repartition(repartition)
  }

  def applyFileReduce(numFiles: Option[Int]): Dataset[_] => Dataset[_] = {
    ds => numFiles.map(ds.repartition).getOrElse(ds)
  }

  def applyPartitionBy(partitionCols: Seq[String]): DataFrameWriter[_] => DataFrameWriter[_] = {
    dfw => if (partitionCols.nonEmpty) dfw.partitionBy(partitionCols: _*) else dfw
  }

  def applyRepartitionAndPartitionBy(partitions: Option[Either[Seq[String], Int]], repartition: Boolean): (Dataset[_] => Dataset[_], DataFrameWriter[_] => DataFrameWriter[_]) = partitions match {
    case None => (e => e, w => w)
    case Some(Left(p)) => (applyRepartition(p, repartition), applyPartitionBy(p))
    case Some(Right(p)) => (applyRepartition(p), w => w)
  }

  def applyMode(mode: SaveMode): DataFrameWriter[_] => DataFrameWriter[_] = dfw => dfw.mode(mode)

  def applyOverwrite(overwrite: Boolean): DataFrameWriter[_] => DataFrameWriter[_] = {
    val mode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
    applyMode(mode)
  }

  def applyWriteParquet(path: String): DataFrameWriter[_] => Unit = dfw => dfw.parquet(path)

  def applyWriteCSV(path: String): DataFrameWriter[_] => Unit = dfw => dfw.csv(path)

  def applySaveAsTable(database: String, table: String): DataFrameWriter[_] => Unit = dfw => dfw.saveAsTable(s"$database.$table")

  /**
    * Base function for all read operation, in all cases users should use more specialised one.
    * This one is used by other builders.
    *
    * @param dataFlow - flow to which to add the write action
    * @param label    - Label of the output Dataset
    * @param open     - dataset opening function
    * @return
    */
  def openBase(dataFlow: SparkDataFlow, label: String)(open: SparkFlowContext => Dataset[_]): SparkDataFlow = {
    def read(): ActionResult = {
      Seq(Some(open(dataFlow.flowContext)))
    }

    dataFlow.addAction(new SimpleAction(List.empty, List(label), _ => read(), "read"))
  }

  def applyOpenDataFrameReader: SparkFlowContext => DataFrameReader = {
    flowContext => flowContext.spark.read
  }

  def applyReaderOptions(options: Map[String, String]): DataFrameReader => DataFrameReader = {
    reader => options.foldLeft(reader)((z, c) => z.option(c._1, c._2))
  }

  def applyOpenCSV(path: String): DataFrameReader => Dataset[_] = {
    reader => reader.csv(path)
  }

  def applyOpenParquet(path: String): DataFrameReader => Dataset[_] = {
    reader => reader.parquet(path)
  }

  def isValidViewName(sparkSession: SparkSession)(label: String): Boolean = {
    Try(sparkSession.sessionState.sqlParser.parseTableIdentifier(label)).isSuccess
  }

  def checkValidSqlLabels(sparkSession: SparkSession, labels: Seq[String], actionName: String): Unit = {
    labels
      .filterNot(isValidViewName(sparkSession))
      .reduceLeftOption((z, l) => s"$z, $l")
      .foreach(l => throw new DataFlowException(s"The following labels contain invalid characters to be used as Spark SQL view names: [$l]. " +
        s"You can alias the label to a valid name before calling the $actionName action."))
  }

}
