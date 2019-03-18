package com.coxautodata.waimak.metastore

import java.sql.ResultSet

import com.coxautodata.waimak.dataflow.DataFlowException
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import scala.util.{Failure, Success, Try}

/**
  * Hive trait that implements the Hive-specific HadoopDBConnector functions
  */
trait HiveDBConnector extends HadoopDBConnector {

  private[metastore] override def createTableFromParquetDDL(tableName: String, pathWithoutUri: String, external: Boolean, partitionColumns: Seq[String], ifNotExists: Boolean = true): Seq[String] = {

    // Qualify the path for this filesystem
    val path = new Path(pathWithoutUri).makeQualified(context.fileSystem.getUri, context.fileSystem.getWorkingDirectory)

    //Find glob paths catering for partitions
    val globPath = {
      if (partitionColumns.isEmpty) new Path(s"$path/part-*.parquet")
      else new Path(path + partitionColumns.mkString("/", "=*/", "=*/part-*.parquet"))
    }

    logInfo("Get paths for ddls " + globPath.toString)
    val parquetFile = context.fileSystem.globStatus(globPath).sortBy(_.getPath.toUri.getPath).headOption.map(_.getPath).getOrElse(throw new DataFlowException(s"Could not find parquet file at " +
      s"'$path' to infer schema for table '$tableName'"))

    //Create ddl
    val ifNotExistsString = if (ifNotExists) "if not exists " else ""
    val externalString = if (external) "external " else ""
    val schemaString = getSchema(parquetFile)
    if (partitionColumns.isEmpty) {
      Seq(s"create ${externalString}table $ifNotExistsString$tableName $schemaString stored as parquet location '$path'")
    } else {
      val partitionDef = partitionColumns.map(_ + " string").mkString(", ")
      Seq(
        s"create ${externalString}table $ifNotExistsString$tableName $schemaString partitioned by ($partitionDef) stored as parquet location '$path'",
        s"alter table $tableName recover partitions"
      )
    }
  }

  override private[metastore] def updateTableLocationDDL(tableName: String, pathWithoutUri: String): String = {
    val path = new Path(pathWithoutUri).makeQualified(context.fileSystem.getUri, context.fileSystem.getWorkingDirectory)
    s"alter table $tableName set location '$path'"
  }

  private[metastore] def getSchema(parquetFile: Path): String = {
    context
        .spark
      .read
      .parquet(parquetFile.toString)
      .schema
      .map(c => s"${c.name} ${c.dataType.typeName}")
      .mkString("(", ", ", ")")
  }
}

/**
  * A dummy Hive database connector that does not submit the DDLs but
  * collects all that have been submitted in a List.
  * This is useful for testing or using in flows where you wish to collect
  * the DDLs and run them manually.
  *
  */
case class HiveDummyConnector(context: SparkFlowContext) extends HiveDBConnector {
  var ranDDLs: List[List[String]] = List.empty

  override private[metastore] def runQueries(ddls: Seq[String]): Seq[Option[ResultSet]] = {
    ranDDLs = ranDDLs :+ ddls.toList
    Seq(None)
  }

  override def getMetadataForTables(tables: Seq[String]): Map[String, TableMetadata] = ???
}

/**
  * A Hive connector that uses the Hive Metastore configured in the SparkSession.
  * Hive support must be enabled on the SparkSession to use this connector.
  * The connector uses the filesystem configured in the SparkFlowContext to discover
  * partitions therefore table paths must exist on that filesystem.
  *
  * @param context          The flow context object containing the SparkSession and FileSystem
  * @param database                  Database to create tables in
  * @param createDatabaseIfNotExists Whether to create the database if it does not exist (default false)
  */
case class HiveSparkSQLConnector(context: SparkFlowContext,
                                 database: String,
                                 createDatabaseIfNotExists: Boolean = false) extends HiveDBConnector {

  override def submitAtomicResultlessQueries(ddls: Seq[String]): Unit = {
    val ddlWithUse = s"use $database" +: ddls
    val allDdls = if (createDatabaseIfNotExists) s"create database if not exists $database" +: ddlWithUse else ddlWithUse
    allDdls.foreach(context.spark.sql)
    Unit
  }

  override private[metastore] def runQueries(ddls: Seq[String]): Seq[Option[ResultSet]] = {
    throw new UnsupportedOperationException(s"${this.getClass.getSimpleName} does not support running queries that return data. You must use SparkSession.sql directly.")
  }

  override def getMetadataForTables(tables: Seq[String]): Map[String, TableMetadata] = {
    import context.spark.implicits._
    submitAtomicResultlessQueries(Seq.empty)
    tables.map{
      t => t -> Try(context.spark.sql(s"show create table $t").as[String].collect().head)
    }
      .toMap
      .mapValues(createTableToTableMetadata)
  }

  private def createTableToTableMetadata(maybeCreateTable: Try[String]): TableMetadata = maybeCreateTable match {
    case Success(v) =>
      val lines = v.lines
      val partitions = lines.filter(_.startsWith("PARTITIONED BY ")).flatMap(l => "`[^`,]+`".r.findAllIn(l).toList)
      val location = lines.filter(_.startsWith("LOCATION")).toList.headOption.flatMap(l => "`[^`]+`".r.findFirstIn(l)).getOrElse(throw new RuntimeException)
      TableMetadata(Some(new Path(location)), partitions.toList)
    case Failure(_:NoSuchTableException) => TableMetadata(None, Seq.empty)
    case Failure(e) => throw e
  }
}