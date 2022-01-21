package com.coxautodata.waimak.metastore

import java.sql.ResultSet

import com.coxautodata.waimak.dataflow.DataFlowException
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import org.apache.hadoop.fs.Path

/**
  * Hive trait that implements the Hive-specific HadoopDBConnector functions
  */
trait HiveDBConnector extends HadoopDBConnector {

  def database: String

  private[metastore] override def createTableFromParquetDDL(tableName: String, pathWithoutUri: String, external: Boolean, partitionColumns: Seq[String], ifNotExists: Boolean = true): Seq[String] = {

    val qualifiedTableName = s"$database.$tableName"

    // Qualify the path for this filesystem
    val path = new Path(pathWithoutUri).makeQualified(context.fileSystem.getUri, context.fileSystem.getWorkingDirectory)

    //Find glob paths catering for partitions
    val globPath = ("part-*.parquet" +: partitionColumns.map(_ + "=*")).foldRight(path)((c, p) => new Path(p, c))

    logInfo("Get paths for ddls " + globPath.toString)
    val parquetFile = context.fileSystem.globStatus(globPath).sortBy(_.getPath.toUri.getPath).headOption.map(_.getPath).getOrElse(throw new DataFlowException(s"Could not find parquet file at " +
      s"'$path' to infer schema for table '$qualifiedTableName'"))

    //Create ddl
    val ifNotExistsString = if (ifNotExists) "if not exists " else ""
    val externalString = if (external) "external " else ""
    val schemaString = getSchema(parquetFile)
    if (partitionColumns.isEmpty) {
      Seq(s"create ${externalString}table $ifNotExistsString$qualifiedTableName $schemaString stored as parquet location '$path'")
    } else {
      val partitionDef = partitionColumns.map(_ + " string").mkString(", ")
      Seq(
        s"create ${externalString}table $ifNotExistsString$qualifiedTableName $schemaString partitioned by ($partitionDef) stored as parquet location '$path'",
        s"alter table $qualifiedTableName recover partitions"
      )
    }
  }

  private[metastore] override def dropTableParquetDDL(tableName: String, ifExistsOption: Boolean): String = {
    val qualifiedTableName = s"$database.$tableName"
    super.dropTableParquetDDL(qualifiedTableName, ifExistsOption)
  }

  override private[metastore] def updateTableLocationDDL(tableName: String, pathWithoutUri: String): String = {
    val qualifiedTableName = s"$database.$tableName"
    val path = new Path(pathWithoutUri).makeQualified(context.fileSystem.getUri, context.fileSystem.getWorkingDirectory)
    s"alter table $qualifiedTableName set location '$path'"
  }

  private[metastore] def getSchema(parquetFile: Path): String = {
    context
      .spark
      .read
      .parquet(parquetFile.toString)
      .schema
      .map(c => s"${c.name} ${c.dataType.catalogString}")
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
  val database: String = "test"
  var ranDDLs: List[List[String]] = List.empty

  override private[metastore] def runQueries(ddls: Seq[String]): Seq[Option[ResultSet]] = {
    ranDDLs = ranDDLs :+ ddls.toList
    Seq(None)
  }
}

/**
  * A Hive connector that uses the Hive Metastore configured in the SparkSession.
  * Hive support must be enabled on the SparkSession to use this connector.
  * The connector uses the filesystem configured in the SparkFlowContext to discover
  * partitions therefore table paths must exist on that filesystem.
  *
  * @param context                   The flow context object containing the SparkSession and FileSystem
  * @param database                  Database to create tables in
  * @param createDatabaseIfNotExists Whether to create the database if it does not exist (default false)
  */
case class HiveSparkSQLConnector(context: SparkFlowContext,
                                 database: String,
                                 createDatabaseIfNotExists: Boolean = false) extends HiveDBConnector {

  override def submitAtomicResultlessQueries(ddls: Seq[String]): Unit = {
    val allDdls = if (createDatabaseIfNotExists) s"create database if not exists $database" +: ddls else ddls
    allDdls.foreach(context.spark.sql)
    ()
  }

  override private[metastore] def runQueries(ddls: Seq[String]): Seq[Option[ResultSet]] = {
    throw new UnsupportedOperationException(s"${this.getClass.getSimpleName} does not support running queries that return data. You must use SparkSession.sql directly.")
  }

}