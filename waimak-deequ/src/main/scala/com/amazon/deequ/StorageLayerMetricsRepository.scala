package com.amazon.deequ

import java.sql.Timestamp
import java.time.{ZoneOffset, ZonedDateTime}

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{AnalysisResult, MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import com.coxautodata.waimak.storage.{AuditTableInfo, Storage}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

/**
  * An implementation of [[MetricsRepository]] which uses the storage layer
  *
  * @param storageBasePath  the base path for the storage layer
  * @param metricsTableName the table name to use for storing metrics
  * @param sparkFlowContext the flow context
  */
class StorageLayerMetricsRepository(storageBasePath: Path, metricsTableName: String, sparkFlowContext: SparkFlowContext) extends MetricsRepository {

  import sparkFlowContext.spark.implicits._

  val info = AuditTableInfo(metricsTableName, Seq("tags"), Map.empty, true)

  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {

    val table = Storage.getOrCreateFileTables(sparkFlowContext.spark, storageBasePath, Seq(metricsTableName), Some(_ => info), false).head
    val ds: Dataset[SerializableAnalysisResult] = sparkFlowContext.spark.createDataset[SerializableAnalysisResult](Seq(
      SerializableAnalysisResult(resultKey.tags.toSeq, new Timestamp(resultKey.dataSetDate), new AnalysisResult(resultKey, analyzerContext))
    ))
    Storage.writeToFileTable(sparkFlowContext, table, ds, "dataSetDateTS", ZonedDateTime.now(ZoneOffset.UTC), (_, _, _) => true)
  }

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = load().get().find(_.resultKey == resultKey).map(_.analyzerContext)

  override def load(): MetricsRepositoryMultipleResultsLoader = {

    val table: Dataset[SerializableAnalysisResult] = Storage
      .getOrCreateFileTables(sparkFlowContext.spark, storageBasePath, Seq(metricsTableName), Some(_ => info), false)
      .head
      .allBetween(None, None)
      .map(_.as[SerializableAnalysisResult])
      .getOrElse(sparkFlowContext.spark.emptyDataset[SerializableAnalysisResult])

    StorageLayerMetricsRepositoryMultipleResultsLoader(table)
  }

}

