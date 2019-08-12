package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.TestSparkData.{basePath, purchases}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, Dataset}

class TestCacheConfigurationExtension extends SparkAndTmpDirSpec {
  override val appName: String = "TestCacheAsParquetConfigurationExtension"

  describe("CacheAsParquetConfigurationExtension") {

    it("No caching") {

      val spark = sparkSession
      spark.conf.set(SparkDataFlow.REMOVE_TEMP_AFTER_EXECUTION, false)

      val flow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .alias("csv_1", "purchases")
        .alias("csv_1", "purchases_2")
        .show("purchases")
        .show("purchases_2")
        .printSchema("purchases")
        .printSchema("purchases_2")
        .debugAsTable("purchases")
        .debugAsTable("purchases_2")

      Waimak.sparkExecutor().execute(flow)

      intercept[AnalysisException] {
        sparkSession.read.parquet(new Path(tmpDir, "purchases").toString)
      }

      intercept[AnalysisException] {
        sparkSession.read.parquet(new Path(tmpDir, "purchases_2").toString)
      }

    }

    it("Cache single label") {

      val spark = sparkSession
      import spark.implicits._
      spark.conf.set(SparkDataFlow.REMOVE_TEMP_AFTER_EXECUTION, false)
      spark.conf.set("spark.waimak.dataflow.extensions", "cacheasparquet")
      spark.conf.set("spark.waimak.dataflow.extensions.cacheasparquet.cacheLabels", "purchases")

      val flow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .alias("csv_1", "purchases")
        .alias("csv_1", "purchases_2")
        .show("purchases")
        .show("purchases_2")
        .printSchema("purchases")
        .printSchema("purchases_2")
        .debugAsTable("purchases")
        .debugAsTable("purchases_2")

      Waimak.sparkExecutor().execute(flow)

      val readBack = sparkSession.read.parquet(new Path(tmpDir, "purchases").toString)
      readBack.show()
      readBack.as[TPurchase].collect() should be(purchases)

      intercept[AnalysisException] {
        sparkSession.read.parquet(new Path(tmpDir, "purchases_2").toString)
      }

    }

    it("Cache all labels") {

      val spark = sparkSession
      import spark.implicits._
      spark.conf.set(SparkDataFlow.REMOVE_TEMP_AFTER_EXECUTION, false)
      spark.conf.set("spark.waimak.dataflow.extensions", "cacheasparquet")
      spark.conf.set("spark.waimak.dataflow.extensions.cacheasparquet.cacheAll", true)

      val flow = Waimak.sparkFlow(sparkSession, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .alias("csv_1", "purchases")
        .alias("csv_1", "purchases_2")
        .show("purchases")
        .show("purchases_2")
        .printSchema("purchases")
        .printSchema("purchases_2")
        .debugAsTable("purchases")
        .debugAsTable("purchases_2")

      Waimak.sparkExecutor().execute(flow)

      val readBack = sparkSession.read.parquet(new Path(tmpDir, "purchases").toString)
      readBack.show()
      readBack.as[TPurchase].collect() should be(purchases)

      val readBack_2 = sparkSession.read.parquet(new Path(tmpDir, "purchases_2").toString)
      readBack_2.show()
      readBack_2.as[TPurchase].collect() should be(purchases)
    }
  }

  describe("SparkCacheConfigurationExtension") {

    it("No caching") {

      val finalState = Waimak.sparkFlow(sparkSession)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "purchases")
        .alias("csv_2", "purchases_2")
        .show("purchases")
        .show("purchases_2")
        .printSchema("purchases")
        .printSchema("purchases_2")
        .execute()

      val maybeCachedPurchases = finalState._2.inputs.get[Dataset[_]]("purchases")
      sparkSession.sharedState.cacheManager.lookupCachedData(maybeCachedPurchases) should be(None)
      val maybeCachedPurchases2 = finalState._2.inputs.get[Dataset[_]]("purchases_2")
      sparkSession.sharedState.cacheManager.lookupCachedData(maybeCachedPurchases2) should be(None)

    }

    it("Cache single label") {

      sparkSession.conf.set("spark.waimak.dataflow.extensions", "sparkcache")
      sparkSession.conf.set("spark.waimak.dataflow.extensions.sparkcache.cacheLabels", "purchases")

      val finalState = Waimak.sparkFlow(sparkSession)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "purchases")
        .alias("csv_2", "purchases_2")
        .show("purchases")
        .show("purchases_2")
        .printSchema("purchases")
        .printSchema("purchases_2")
        .execute()

      val maybeCachedPurchases2 = finalState._2.inputs.get[Dataset[_]]("purchases_2")
      sparkSession.sharedState.cacheManager.lookupCachedData(maybeCachedPurchases2).isDefined should be(false)

      val maybeCachedPurchases = finalState._2.inputs.get[Dataset[_]]("purchases")
      sparkSession.sharedState.cacheManager.lookupCachedData(maybeCachedPurchases).isDefined should be(true)
    }

    it("Cache all labels") {

      sparkSession.conf.set("spark.waimak.dataflow.extensions", "sparkcache")
      sparkSession.conf.set("spark.waimak.dataflow.extensions.sparkcache.cacheAll", "true")

      val finalState = Waimak.sparkFlow(sparkSession)
        .openCSV(basePath)("csv_1", "csv_2")
        .alias("csv_1", "purchases")
        .alias("csv_2", "purchases_2")
        .show("purchases")
        .show("purchases_2")
        .printSchema("purchases")
        .printSchema("purchases_2")
        .execute()

      val maybeCachedPurchases2 = finalState._2.inputs.get[Dataset[_]]("purchases_2")
      sparkSession.sharedState.cacheManager.lookupCachedData(maybeCachedPurchases2).isDefined should be(true)

      val maybeCachedPurchases = finalState._2.inputs.get[Dataset[_]]("purchases")
      sparkSession.sharedState.cacheManager.lookupCachedData(maybeCachedPurchases).isDefined should be(true)

    }
  }


}
