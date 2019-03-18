package com.coxautodata.waimak.dataflow.spark

import java.io.FileNotFoundException
import java.util.UUID

import com.coxautodata.waimak.dataflow.{ActionResult, DataFlowEntities}
import com.coxautodata.waimak.log.Logging
import com.coxautodata.waimak.metastore.{HadoopDBConnector, TableMetadata}
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path, PathOperationException}

import scala.util.Try

private[spark] object CommitActions {

  implicit class CommitActionImplicits(sparkDataFlow: SparkDataFlow) {

    def commitLabels(labelsToCommitDefinitions: Map[String, LabelCommitDefinition], commitTempPath: Path): SparkDataFlow = {
      val randomID = UUID.randomUUID().toString

      val committedFilesFlow: SparkDataFlow = sparkDataFlow
        .tag(randomID) {
          _.addAction(CommitFilesAction(labelsToCommitDefinitions, commitTempPath))
        }

      labelsToCommitDefinitions
        .filter(_._2.connection.isDefined)
        .groupBy(_._2.connection.get)
        .foldLeft(committedFilesFlow) {
          case (z, (c, l)) =>
            z.tagDependency(randomID) {
              _.addAction(CurrentMetadataQuery(c, l.keys.toList))
            }.addNewTableMetadata(l)
              .compareTableSchemas(l.keys.toList)
        }
    }

    private[spark] def addNewTableMetadata(labels: Map[String, LabelCommitDefinition]): SparkDataFlow = {
      labels.foldLeft(sparkDataFlow) {
        case (z, (table, definition)) => z.addInput(s"${table}_NEW_TABLE_METADATA", Some(TableMetadata(Some(definition.outputPath), definition.partitions)))
      }
    }

    private[spark] def compareTableSchemas(labels: List[String]): SparkDataFlow = {
      labels.foldLeft(sparkDataFlow) {
        case (z, t) => z.addAction(CompareTableSchemas(t))
      }

    }

  }

}

private[spark] case class CommitDDLs(connector: HadoopDBConnector, labels: Map[String, LabelCommitDefinition]) extends SparkDataFlowAction {

  override val inputLabels: List[String] = labels.keys.map(l => s"${l}_SCHEMA_CHANGED").toList
  override val outputLabels: List[String] = List.empty

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try {
    labels.values.flatMap {
      d =>
        val schemaChanged = inputs.get[Boolean](s"${d.labelName}_SCHEMA_CHANGED")
        connector.updateTableParquetLocationDDLs(d.labelName, d.outputPath.toUri.getPath, d.partitions, schemaChanged)
    }
  }
    .map {
      ddls =>
        connector.submitAtomicResultlessQueries(ddls.toList)
        Seq.empty
    }


}

private[spark] case class CompareTableSchemas(table: String) extends SparkDataFlowAction {

  val currentMetadataLabel = s"${table}_CURRENT_TABLE_METADATA"
  val newMetadataLabel = s"${table}_NEW_TABLE_METADATA"

  override val inputLabels: List[String] = List(currentMetadataLabel, newMetadataLabel)
  override val outputLabels: List[String] = List(s"${table}_SCHEMA_CHANGED")

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try {

    def getSchema(path: Path): String = flowContext.spark.read.parquet(path.toString).schema.json

    val currMeta = inputs.get[TableMetadata](currentMetadataLabel)
    val newMeta = inputs.get[TableMetadata](newMetadataLabel)

    Seq(Some(currMeta.path.isEmpty || (currMeta.partitions != newMeta.partitions) || (getSchema(currMeta.path.get) != getSchema(newMeta.path.get))))

  }

}

private[spark] case class CurrentMetadataQuery(conn: HadoopDBConnector, tables: List[String]) extends SparkDataFlowAction {

  override val inputLabels: List[String] = List.empty
  override val outputLabels: List[String] = tables.map(t => s"${t}_CURRENT_TABLE_METADATA")

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try {
    val meta = conn.getMetadataForTables(tables)
    tables.map(t => Some(meta(t)))
  }

}

private[spark] case class CommitFilesAction(commitLabels: Map[String, LabelCommitDefinition], tempPath: Path) extends SparkDataFlowAction with Logging {

  val inputLabels: List[String] = commitLabels.keys.toList
  val outputLabels: List[String] = List.empty

  override val requiresAllInputs = false

  override def performAction(inputs: DataFlowEntities, flowContext: SparkFlowContext): Try[ActionResult] = Try {

    // Create path objects
    val srcDestMap: Map[String, (Path, Path)] = commitLabels.map {
      case (tableName, destDef) =>
        val srcPath = new Path(tempPath, tableName)
        val destPath = destDef.outputPath
        if (!flowContext.fileSystem.exists(srcPath)) throw new FileNotFoundException(s"Cannot commit table $tableName as " +
          s"the source path does not exist: ${srcPath.toUri.getPath}")
        if (flowContext.fileSystem.exists(destPath)) throw new FileAlreadyExistsException(s"Cannot commit table $tableName as " +
          s"the destination path already exists: ${destPath.toUri.getPath}")
        tableName -> (srcPath, destPath)
    }

    // Directory moving
    srcDestMap.foreach {
      case (label, (srcPath, destPath)) =>
        if (!flowContext.fileSystem.exists(destPath.getParent)) {
          logInfo(s"Creating parent folder ${destPath.getParent.toUri.getPath} for label $label")
          val res = flowContext.fileSystem.mkdirs(destPath.getParent)
          if (!res) throw new PathOperationException(s"Could not create parent directory: ${destPath.getParent.toUri.getPath} for label $label")
        }
        val res = flowContext.fileSystem.rename(srcPath, destPath)
        if (!res) throw new PathOperationException(s"Could not move path ${srcPath.toUri.getPath} to ${destPath.toUri.getPath} for label $label")
    }

    List.empty
  }
}