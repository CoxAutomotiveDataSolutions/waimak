package com.coxautodata.waimak.metastore

import java.sql.ResultSet

import com.coxautodata.waimak.dataflow.DataFlowException
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * Impala trait that implements the Impala-specific HadoopDBConnector functions
  */
trait ImpalaDBConnector extends HadoopDBConnector {

  def sparkSession: SparkSession

  lazy val fileSystem: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

  private[metastore] override def createTableFromParquetDDL(tableName: String, path: String, external: Boolean, partitionColumns: Seq[String], ifNotExists: Boolean = true): Seq[String] = {
    //Find glob paths catering for partitions
    val globPath = {
      if (partitionColumns.isEmpty) new Path(s"$path/part-*.parquet")
      else new Path(path + partitionColumns.mkString("/", "=*/", "=*/part-*.parquet"))
    }

    logInfo("Get paths for ddls " + globPath.toString)
    val parquetFile = fileSystem.globStatus(globPath).sortBy(_.getPath.toUri.getPath).headOption.map(_.getPath).getOrElse(throw new DataFlowException(s"Could not find parquet file at " +
      s"'$path' to infer schema for table '$tableName'"))

    //Create ddl
    val ifNotExistsString = if (ifNotExists) "if not exists " else ""
    val externalString = if (external) "external " else ""
    if (partitionColumns.isEmpty) {
      Seq(s"create ${externalString}table $ifNotExistsString$tableName like parquet '${parquetFile.toString}' stored as parquet location '$path'")
    } else {
      val partitionDef = partitionColumns.map(_ + " string").mkString(", ")
      Seq(
        s"create ${externalString}table $ifNotExistsString$tableName like parquet '${parquetFile.toString}' partitioned by ($partitionDef) stored as parquet location '$path'",
        s"alter table $tableName recover partitions"
      )
    }
  }
}

/**
  * Impala Database connector that is constructed using the Waimak JDBC template in spark conf
  *
  * @param sparkSession SparkSession object
  * @param database     name of the database to connect to
  * @param cluster      the cluster label in the JDBC template string
  */
case class ImpalaWaimakJDBCConnector(sparkSession: SparkSession,
                                     database: String,
                                     cluster: String = "default",
                                     forceRecreateTables: Boolean = false,
                                     properties: java.util.Properties = new java.util.Properties(),
                                     secureProperties: Map[String, String] = Map.empty) extends ImpalaDBConnector with WaimakJDBCConnector {
  override val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  override val sparkConf: SparkConf = sparkSession.sparkContext.getConf
  override val service: String = "impala"

  override def hadoopConfiguration: Configuration = sparkSession.sparkContext.hadoopConfiguration
}

/**
  * Impala Database connector that is contructed from a JDBC connection string
  *
  * @param sparkSession SparkSession object
  * @param jdbcString   the JDBC connection string
  */
case class ImpalaJDBCConnector(sparkSession: SparkSession,
                               jdbcString: String,
                               forceRecreateTables: Boolean,
                               properties: java.util.Properties = new java.util.Properties(),
                               secureProperties: Map[String, String] = Map.empty) extends ImpalaDBConnector with JDBCConnector {
  override val driverName: String = "org.apache.hive.jdbc.HiveDriver"

  override def hadoopConfiguration: Configuration = sparkSession.sparkContext.hadoopConfiguration
}

/**
  * A dummy Impala database connector that does not submit the DDLs but
  * collects all that have been submitted in a List.
  * This is useful for testing or using in flows where you wish to collect
  * the DDLs and run them manually.
  *
  * @param sparkSession SparkSession object
  */
case class ImpalaDummyConnector(sparkSession: SparkSession, forceRecreateTables: Boolean = false) extends ImpalaDBConnector {
  var ranDDLs: List[List[String]] = List.empty

  override private[metastore] def runQueries(ddls: Seq[String]): Seq[Option[ResultSet]] = {
    ranDDLs = ranDDLs :+ ddls.toList
    Seq(None)
  }
}