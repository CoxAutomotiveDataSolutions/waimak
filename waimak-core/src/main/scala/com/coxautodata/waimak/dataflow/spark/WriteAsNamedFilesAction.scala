package com.coxautodata.waimak.dataflow.spark

import java.util.UUID

import com.coxautodata.waimak.dataflow.spark.WriteAsNamedFilesAction._
import com.coxautodata.waimak.dataflow.{ActionResult, DataFlowEntities, DataFlowException}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset

import scala.util.Try

case class WriteAsNamedFilesAction(label: String, tempBasePath: Path, destBasePath: Path, numberOfFiles: Int, filenamePrefix: String, format: String) extends SparkDataFlowAction {

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try {

    val tempPath = new Path(tempBasePath, UUID.randomUUID().toString)
    inputs.get[Dataset[_]](label).repartition(numberOfFiles).write.format(format).save(tempPath.toString)

    val foundFiles = flowContext.fileSystem.globStatus(new Path(tempPath, s"part-*.*$format*"))
    if (numberOfFiles != foundFiles.length) throw new DataFlowException(s"Number of files found [${foundFiles.length}] did not match requested number of files [$numberOfFiles]")
    foundFiles
      .zip {
        if (numberOfFiles == 1) Stream.continually("")
        else Stream.from(1).map(i => s".${s"%0${numberOfFiles.toString.length}d".format(i)}.")
      }
      .foreach {
        case (sourceFile, number) =>
          val destPath = new Path(destBasePath, s"${filenamePrefix}${number}${getOutputExtension(sourceFile.getPath)}")
          flowContext.fileSystem.rename(sourceFile.getPath, destPath)
      }
    List.empty
  }

  override val inputLabels: List[String] = List.empty
  override val outputLabels: List[String] = List.empty
}

object WriteAsNamedFilesAction {
  def getOutputExtension(path: Path): String = path.getName.dropWhile(_ != '.')
}
