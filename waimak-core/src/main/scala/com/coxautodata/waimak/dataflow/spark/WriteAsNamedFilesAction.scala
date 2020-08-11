package com.coxautodata.waimak.dataflow.spark

import java.util.UUID

import com.coxautodata.waimak.dataflow.spark.WriteAsNamedFilesAction._
import com.coxautodata.waimak.dataflow.{ActionResult, DataFlowEntities, DataFlowException}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrameWriter, Dataset}

import scala.util.Try

/**
  * Write a file or files with a specific filename to a folder.
  * Allows you to control the final output filename without the Spark-generated part UUIDs.
  * Filename will be `$filenamePrefix.extension` if number of files is 1, otherwise
  * `$filenamePrefix.$fileNumber.extension` where file number is incremental and zero-padded.
  *
  * @param label          Label to write
  * @param tempBasePath   Base location of temporary folder
  * @param destBasePath   Destination path to put files in
  * @param numberOfFiles  Number of files to generate
  * @param filenamePrefix Prefix of name of the file up to the filenumber and extension
  * @param format         Format to write (e.g. parquet, csv)
  * @param options        Options to pass to the [[DataFrameWriter]]
  */
case class WriteAsNamedFilesAction(label: String, tempBasePath: Path, destBasePath: Path, numberOfFiles: Int, filenamePrefix: String, format: String, options: Map[String, String]) extends SparkDataFlowAction {

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try {

    checkTextWriteOptions(numberOfFiles, format)
    val tempPath = new Path(tempBasePath, UUID.randomUUID().toString)
    inputs.get[Dataset[_]](label).repartition(numberOfFiles).write.options(options).format(format).save(tempPath.toString)

    val foundFiles = flowContext.fileSystem.globStatus(new Path(tempPath, s"part-*.*${fixTextExtension(format)}*"))
    if (numberOfFiles != foundFiles.length) throw new DataFlowException(s"Number of files found [${foundFiles.length}] did not match requested number of files [$numberOfFiles]")
    foundFiles
      .zip {
        if (numberOfFiles == 1) Stream.continually("")
        else Stream.from(1).map(i => s".${s"%0${numberOfFiles.toString.length}d".format(i)}")
      }
      .foreach {
        case (sourceFile, number) =>
          flowContext.fileSystem.mkdirs(destBasePath)
          val destPath = new Path(destBasePath, s"${filenamePrefix}${number}${getOutputExtension(sourceFile.getPath)}")
          if (!flowContext.fileSystem.rename(sourceFile.getPath, destPath)) throw new DataFlowException(s"Failed to move file [${sourceFile.getPath}] to [$destPath]")
      }
    List.empty
  }

  override val inputLabels: List[String] = List(label)
  override val outputLabels: List[String] = List.empty
}

object WriteAsNamedFilesAction {
  def checkTextWriteOptions(numberOfFiles: Int, format: String): Unit = {
    if (format == "text") {
      if (numberOfFiles > 1) throw new IllegalArgumentException("When writing text files only 1 file is able to be written")
    }
  }

  def fixTextExtension(ext: String): String = ext match {
    case "text" => "txt"
    case _ @ a => a
  }

  def getOutputExtension(path: Path): String = path.getName.dropWhile(_ != '.')
}
