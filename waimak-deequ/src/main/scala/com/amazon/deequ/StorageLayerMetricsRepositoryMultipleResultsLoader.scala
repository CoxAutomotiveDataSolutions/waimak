package com.amazon.deequ

import java.sql.Timestamp

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.{AnalysisResult, AnalysisResultSerde, MetricsRepositoryMultipleResultsLoader}
import org.apache.spark.sql.Dataset

case class StorageLayerMetricsRepositoryMultipleResultsLoader(ds: Dataset[SerializableAnalysisResult],
                                                              tagValues: Option[Map[String, String]] = None,
                                                              forAnalyzers: Option[Seq[Analyzer[_, Metric[_]]]] = None,
                                                              before: Option[Long] = None,
                                                              after: Option[Long] = None
                                                             ) extends MetricsRepositoryMultipleResultsLoader {
  override def withTagValues(tagValues: Map[String, String]): MetricsRepositoryMultipleResultsLoader = this.copy(tagValues = Some(tagValues))

  override def forAnalyzers(analyzers: Seq[Analyzer[_, Metric[_]]]): MetricsRepositoryMultipleResultsLoader = this.copy(forAnalyzers = Some(analyzers))

  override def after(dateTime: Long): MetricsRepositoryMultipleResultsLoader = this.copy(after = Some(dateTime))

  override def before(dateTime: Long): MetricsRepositoryMultipleResultsLoader = this.copy(before = Some(dateTime))

  override def get(): Seq[AnalysisResult] = {
    ds
      .filter { result => after.isEmpty || after.get <= result.dataSetDate }
      .filter { result => before.isEmpty || result.dataSetDate <= before.get }
      .filter { result =>
        tagValues.isEmpty ||
          tagValues.get.toSet.subsetOf(result.tags.toSet)
      }
      .collect()
      .toSeq
      .flatMap(_.analysisResult)
      .map { analysisResult =>

        val requestedMetrics = analysisResult
          .analyzerContext
          .metricMap
          .filterKeys(analyzer => forAnalyzers.isEmpty || forAnalyzers.get.contains(analyzer))

        val requestedAnalyzerContext = AnalyzerContext(requestedMetrics)

        new AnalysisResult(analysisResult.resultKey, requestedAnalyzerContext)

      }
  }
}

case class SerializableAnalysisResult(tags: Seq[(String, String)], dataSetDateTS: Timestamp, serializedAnalysisResult: String) {
  def dataSetDate: Long = dataSetDateTS.getTime

  def analysisResult: Seq[AnalysisResult] = AnalysisResultSerde.deserialize(serializedAnalysisResult)

}

object SerializableAnalysisResult {
  def apply(tags: Seq[(String, String)], dataSetDateTS: Timestamp, analysisResult: AnalysisResult): SerializableAnalysisResult = SerializableAnalysisResult(tags, dataSetDateTS, AnalysisResultSerde.serialize(Seq(analysisResult)))
}