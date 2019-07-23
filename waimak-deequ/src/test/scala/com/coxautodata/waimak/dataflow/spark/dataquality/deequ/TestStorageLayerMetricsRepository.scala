package com.coxautodata.waimak.dataflow.spark.dataquality.deequ

import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.anomalydetection.RateOfChangeStrategy
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.{StorageLayerMetricsRepository, VerificationSuite}
import com.coxautodata.waimak.dataflow.spark.{SparkAndTmpDirSpec, SparkFlowContext}
import org.apache.hadoop.fs.Path

class TestStorageLayerMetricsRepository extends SparkAndTmpDirSpec {
  override val appName: String = "TestStorageLayerMetricsRepository"

  it("StorageLayerMetricsRepository") {
    val spark = sparkSession
    import spark.implicits._
    val context = SparkFlowContext(spark)
    val metricsRepository = new StorageLayerMetricsRepository(new Path(testingBaseDirName), "metrics", context)

    val yesterdaysDataset = Seq(
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0)).toDF()

    val yesterdaysKey = ResultKey(System.currentTimeMillis() - 24 * 60 * 1000)

    VerificationSuite()
      .onData(yesterdaysDataset)
      .useRepository(metricsRepository)
      .saveOrAppendResult(yesterdaysKey)
      .addAnomalyCheck(
        RateOfChangeStrategy(maxRateIncrease = Some(2.0)),
        Size())
      .run()

    val todaysDataset = Seq(
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12)
    ).toDF()

    val todaysKey = ResultKey(System.currentTimeMillis())

    val verificationResult = VerificationSuite()
      .onData(todaysDataset)
      .useRepository(metricsRepository)
      .saveOrAppendResult(todaysKey)
      .addAnomalyCheck(
        RateOfChangeStrategy(maxRateIncrease = Some(2.0)),
        Size())
      .run()

    if (verificationResult.status != CheckStatus.Success) {
      println("Anomaly detected in the Size() metric!")

      metricsRepository
        .load()
        .forAnalyzers(Seq(Size()))
        .getSuccessMetricsAsDataFrame(spark)
        .show()
    }
  }
}

case class Item(
                 id: Long,
                 name: String,
                 description: String,
                 priority: String,
                 numViews: Long
               )