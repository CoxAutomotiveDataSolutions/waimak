package com.coxautodata.waimak.dataflow.spark

import com.coxautodata.waimak.dataflow.Waimak
import com.coxautodata.waimak.dataflow.spark.TestSparkData.basePath
import org.apache.hadoop.fs.Path

class TestWriteAsNamedFilesAction extends SparkAndTmpDirSpec {
  override val appName: String = "TestCopyFilesAction"

  describe ("writeAndCopy"){

    it("write a single parquet file"){
      val spark = sparkSession
      import spark.implicits._

      val outputBasePath = new Path(testingBaseDirName, "output")

      Waimak.sparkFlow(spark)
        .openCSV(basePath)("csv_1")
        .writeAsNamedFiles("csv_1", outputBasePath.toString, 1, "files", "parquet")
        .execute()

    }

  }
}
