package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrameWriter, Dataset, SaveMode}

/**
  * Defines builder functions that add various interceptors to a [[SparkDataFlow]]
  *
  * Created by Alexei Perelighin on 2018/02/24
  */
object SparkInterceptors extends Logging {

  type CacheActionType = CachePostAction[Dataset[_], SparkFlowContext]

  def addPostAction(sparkFlow: SparkDataFlow
                    , outputLabel: String
                    , postAction: PostAction[Any, SparkFlowContext]): SparkDataFlow = {
    val toIntercept: DataFlowAction[SparkFlowContext] = sparkFlow.getActionByOutputLabel(outputLabel)
    val interceptor = toIntercept match {
      // Interceptor already exists
      case i@PostActionInterceptor(_, _) => i.addPostAction(postAction)
      // First interceptor
      case _ => PostActionInterceptor(toIntercept, Seq(postAction))
    }
    sparkFlow.addInterceptor(interceptor, toIntercept.guid)
  }

  def addPostCacheAsParquet(sparkFlow: SparkDataFlow, outputLabel: String)
                           (dfFunc: Dataset[_] => Dataset[_])
                           (dfwFunc: DataFrameWriter[_] => DataFrameWriter[_]): SparkDataFlow = {
    def post(data: Option[Any], sfc: SparkFlowContext): Option[Dataset[_]] = {
      logInfo(s"About to cache the $outputLabel. Dataset is defined: ${data.isDefined}")
      val baseFolder = sparkFlow.tempFolder.getOrElse(throw new DataFlowException("Cannot cache, temporary folder was not specified"))
      data
        .map(checkIfDataset(_, outputLabel, "addPostCacheAsParquet"))
        .map(dfFunc)
        .map { ds =>
          val path = new Path(new Path(baseFolder.toString), outputLabel).toString
          dfwFunc(ds.write).mode(SaveMode.Overwrite).parquet(path)
          sfc.spark.read.parquet(path)
        }
    }

    addPostAction(sparkFlow, outputLabel, CachePostAction(post, outputLabel))
  }

  def addCacheAsParquet(sparkFlow: SparkDataFlow, outputLabel: String, partitions: Seq[String] = Seq.empty, repartition: Boolean = true): SparkDataFlow = {
    addPostCacheAsParquet(sparkFlow, outputLabel) {
      e =>
        if (partitions.nonEmpty && repartition) e.repartition(partitions.map(e(_)): _*)
        else e
    } {
      w =>
        if (partitions.nonEmpty) w.partitionBy(partitions: _*)
        else w
    }
  }

  def addPostTransform(sparkFlow: SparkDataFlow, outputLabel: String)(transform: Dataset[_] => Dataset[_]): SparkDataFlow = {
    def post(data: Option[Any], sfc: SparkFlowContext): Option[Dataset[_]] =
      data
        .map(checkIfDataset(_, outputLabel, "addPostTransform"))
        .map(transform)

    addPostAction(sparkFlow, outputLabel, TransformPostAction(post, outputLabel))
  }

  def checkIfDataset(value: Any, label: String, attemptedOperation: String): Dataset[_] = {
    if (!value.isInstanceOf[Dataset[_]]) throw new DataFlowException(s"Can only call $attemptedOperation on a Dataset. Label $label is a ${value.getClass}")
    else value.asInstanceOf[Dataset[_]]
  }
}
