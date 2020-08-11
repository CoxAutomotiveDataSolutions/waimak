package com.coxautodata.waimak.dataflow.spark

import java.io.File

import com.coxautodata.waimak.dataflow.{DataFlowException, Waimak}
import com.coxautodata.waimak.dataflow.spark.TestSparkData.basePath
import org.apache.hadoop.fs.Path

class TestWriteAsNamedFilesAction extends SparkAndTmpDirSpec {
  override val appName: String = "TestCopyFilesAction"

  describe("writeAsNamedFiles") {

    it("write a single parquet file") {
      val spark = sparkSession

      val outputBasePath = new Path(testingBaseDirName, "output")

      Waimak.sparkFlow(spark, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .writeAsNamedFiles("csv_1", outputBasePath.toString, 1, "file", "parquet")
        .execute()

      new File(outputBasePath.toString).listFiles().map(_.getName) should contain theSameElementsAs List("file.snappy.parquet", ".file.snappy.parquet.crc")

    }

    it("write two parquet files") {
      val spark = sparkSession

      val outputBasePath = new Path(testingBaseDirName, "output")

      Waimak.sparkFlow(spark, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .writeAsNamedFiles("csv_1", outputBasePath.toString, 2, "file", "parquet")
        .execute()

      new File(outputBasePath.toString).listFiles().map(_.getName) should contain theSameElementsAs List("file.1.snappy.parquet", ".file.1.snappy.parquet.crc", "file.2.snappy.parquet", ".file.2.snappy.parquet.crc")

    }

    it("write ten parquet files") {
      val spark = sparkSession
      import org.apache.spark.sql.functions.monotonically_increasing_id
      val outputBasePath = new Path(testingBaseDirName, "output")

      Waimak.sparkFlow(spark, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .inPlaceTransform("csv_1")(List.fill(10)(_).reduce(_ union _).withColumn("col3", monotonically_increasing_id))
        .writeAsNamedFiles("csv_1", outputBasePath.toString, 10, "file", "parquet")
        .execute()

      new File(outputBasePath.toString).listFiles().map(_.getName) should contain theSameElementsAs List(
        "file.01.snappy.parquet", ".file.01.snappy.parquet.crc",
        "file.02.snappy.parquet", ".file.02.snappy.parquet.crc",
        "file.03.snappy.parquet", ".file.03.snappy.parquet.crc",
        "file.04.snappy.parquet", ".file.04.snappy.parquet.crc",
        "file.05.snappy.parquet", ".file.05.snappy.parquet.crc",
        "file.06.snappy.parquet", ".file.06.snappy.parquet.crc",
        "file.07.snappy.parquet", ".file.07.snappy.parquet.crc",
        "file.08.snappy.parquet", ".file.08.snappy.parquet.crc",
        "file.09.snappy.parquet", ".file.09.snappy.parquet.crc",
        "file.10.snappy.parquet", ".file.10.snappy.parquet.crc"
      )

    }

    it("write a single csv file") {
      val spark = sparkSession

      val outputBasePath = new Path(testingBaseDirName, "output")

      Waimak.sparkFlow(spark, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .writeAsNamedFiles("csv_1", outputBasePath.toString, 1, "file", "csv", Map("header" -> "true"))
        .execute()

      new File(outputBasePath.toString).listFiles().map(_.getName) should contain theSameElementsAs List("file.csv", ".file.csv.crc")

    }

    it("write two csv files") {
      val spark = sparkSession

      val outputBasePath = new Path(testingBaseDirName, "output")

      Waimak.sparkFlow(spark, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .writeAsNamedFiles("csv_1", outputBasePath.toString, 2, "file", "csv", Map("header" -> "true"))
        .execute()

      new File(outputBasePath.toString).listFiles().map(_.getName) should contain theSameElementsAs List("file.1.csv", ".file.1.csv.crc", "file.2.csv", ".file.2.csv.crc")

    }

    it("write a single gzip'd csv file") {
      val spark = sparkSession

      val outputBasePath = new Path(testingBaseDirName, "output")

      Waimak.sparkFlow(spark, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .writeAsNamedFiles("csv_1", outputBasePath.toString, 1, "file", "csv", Map("header" -> "true", "compression" -> "gzip"))
        .execute()

      new File(outputBasePath.toString).listFiles().map(_.getName) should contain theSameElementsAs List("file.csv.gz", ".file.csv.gz.crc")

    }

    it("write two gzip'd csv files") {
      val spark = sparkSession

      val outputBasePath = new Path(testingBaseDirName, "output")

      Waimak.sparkFlow(spark, tmpDir.toString)
        .openCSV(basePath)("csv_1")
        .writeAsNamedFiles("csv_1", outputBasePath.toString, 2, "file", "csv", Map("header" -> "true", "compression" -> "gzip"))
        .execute()

      new File(outputBasePath.toString).listFiles().map(_.getName) should contain theSameElementsAs List("file.1.csv.gz", ".file.1.csv.gz.crc", "file.2.csv.gz", ".file.2.csv.gz.crc")

    }

    it("write a single text file") {
      val spark = sparkSession
      import spark.implicits._

      val outputBasePath = new Path(testingBaseDirName, "output")

      val data = Seq("hi", "there").toDF("row1")

      Waimak.sparkFlow(spark, tmpDir.toString)
        .open("csv_1", (_ : SparkFlowContext) => data)
        .writeAsNamedFiles("csv_1", outputBasePath.toString, 1, "file", "text", Map("header" -> "true"))
        .execute()

      new File(outputBasePath.toString).listFiles().map(_.getName) should contain theSameElementsAs List("file.txt", ".file.txt.crc")

    }

    it("throw when asking to write multiple text files") {
      val spark = sparkSession
      import spark.implicits._

      val outputBasePath = new Path(testingBaseDirName, "output")

      val data = Seq("hi", "there").toDF("row1")

      an [DataFlowException] should be thrownBy (
        Waimak.sparkFlow(spark, tmpDir.toString)
        .open("csv_1", (_ : SparkFlowContext) => data)
        .writeAsNamedFiles("csv_1", outputBasePath.toString, 2, "file", "text", Map("header" -> "true"))
        .execute()
        )
    }
  }
}
