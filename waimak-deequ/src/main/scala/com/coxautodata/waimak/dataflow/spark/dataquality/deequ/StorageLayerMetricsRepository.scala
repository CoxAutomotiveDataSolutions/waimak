package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import org.apache.hadoop.fs.Path

class StorageLayerMetricsRepository(storageBasePath: Path, metricsTableName: String) extends MetricsRepository {

  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val table = ???
  }

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = ???

  override def load(): MetricsRepositoryMultipleResultsLoader = ???

}
