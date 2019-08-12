package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow._
import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrameWriter, Dataset, SaveMode}
import com.coxautodata.waimak.dataflow.spark.SparkActionHelpers._
import org.apache.spark.storage.StorageLevel
/**
  * Defines builder functions that add various interceptors to a [[SparkDataFlow]]
  *
  * Created by Alexei Perelighin on 2018/02/24
  */
object SparkInterceptors extends Logging {

  type CacheActionType = CachePostAction[Dataset[_]]

  def addPostAction(sparkFlow: SparkDataFlow
                    , outputLabel: String
                    , postAction: PostAction[Any]): SparkDataFlow = {
    val toIntercept: DataFlowAction = sparkFlow.getActionByOutputLabel(outputLabel)
    val interceptor = toIntercept match {
      // Interceptor already exists
      case i@PostActionInterceptor(_, _) => i.addPostAction(postAction)
      // First interceptor
      case _ => PostActionInterceptor(toIntercept, Seq(postAction))
    }
    sparkFlow.addInterceptor(interceptor, toIntercept.guid)
  }

  def addSparkCache(sparkFlow: SparkDataFlow, outputLabel: String, storageLevel: StorageLevel, dfFunc: Dataset[_] => Dataset[_]): SparkDataFlow = {
    def post(data: Option[Any]): Option[Dataset[_]] = {
      logInfo(s"About to cache the $outputLabel. Dataset is defined: ${data.isDefined}")
      data
        .map(checkIfDataset(_, outputLabel, "sparkCache"))
        .map(dfFunc)
        .map { ds =>
          val cached = ds.persist(storageLevel)
          //force cache to happen here by calling inexpensive action
          cached.rdd.isEmpty()
          cached
        }
    }

    addPostAction(sparkFlow, outputLabel, CachePostAction(post, outputLabel))
  }

  def addPostCacheAsParquet(sparkFlow: SparkDataFlow, outputLabel: String)
                           (dfFunc: Dataset[_] => Dataset[_])
                           (dfwFunc: DataFrameWriter[_] => DataFrameWriter[_]): SparkDataFlow = {
    def post(data: Option[Any]): Option[Dataset[_]] = {
      logInfo(s"About to cache the $outputLabel. Dataset is defined: ${data.isDefined}")
      val baseFolder = sparkFlow.tempFolder.getOrElse(throw new DataFlowException("Cannot cache, temporary folder was not specified"))
      data
        .map(checkIfDataset(_, outputLabel, "cacheAsParquet"))
        .map(dfFunc)
        .map { ds =>
          val path = new Path(new Path(baseFolder.toString), outputLabel).toString
          dfwFunc andThen applyOverwrite(true) andThen applyWriteParquet(path) apply ds.write
          applyOpenParquet(path) apply ds.sparkSession.read
        }
    }

    addPostAction(sparkFlow, outputLabel, CachePostAction(post, outputLabel))
  }

  def addPostTransform(sparkFlow: SparkDataFlow, outputLabel: String)(transform: Dataset[_] => Dataset[_]): SparkDataFlow = {
    def post(data: Option[Any]): Option[Dataset[_]] =
      data
        .map(checkIfDataset(_, outputLabel, "inPlaceTransform"))
        .map(transform)

    addPostAction(sparkFlow, outputLabel, TransformPostAction(post, outputLabel))

  }

  def checkIfDataset(value: Any, label: String, attemptedOperation: String): Dataset[_] = {
    if (!value.isInstanceOf[Dataset[_]]) throw new DataFlowException(s"Can only call $attemptedOperation on a Dataset. Label $label is a ${value.getClass.getName}")
    else value.asInstanceOf[Dataset[_]]
  }
}
