package com.coxautodata.waimak.metastore

import java.sql.ResultSet

import com.coxautodata.waimak.dataflow.DataFlowException
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf


/**
  * Impala trait that implements the Impala-specific HadoopDBConnector functions
  */
trait ImpalaDBConnector extends HadoopDBConnector {

  private[metastore] override def createTableFromParquetDDL(tableName: String, path: String, external: Boolean, partitionColumns: Seq[String], ifNotExists: Boolean = true): Seq[String] = {
    val qualifiedPath = new Path(path).makeQualified(context.fileSystem.getUri, context.fileSystem.getWorkingDirectory)

    //Find glob paths catering for partitions
    val globPath = ("part-*.parquet" +: partitionColumns.map(_ + "=*")).foldRight(qualifiedPath)((c, p) => new Path(p, c))

    logInfo("Get paths for ddls " + globPath.toString)
    val parquetFile = context.fileSystem.globStatus(globPath).sortBy(_.getPath.toUri.getPath).headOption.map(_.getPath).getOrElse(throw new DataFlowException(s"Could not find parquet file at " +
      s"'$qualifiedPath' to infer schema for table '$tableName'"))

    //Create ddl
    val ifNotExistsString = if (ifNotExists) "if not exists " else ""
    val externalString = if (external) "external " else ""
    if (partitionColumns.isEmpty) {
      Seq(s"create ${externalString}table $ifNotExistsString$tableName like parquet '${parquetFile.toString}' stored as parquet location '$qualifiedPath'")
    } else {
      val partitionDef = partitionColumns.map(_ + " string").mkString(", ")
      Seq(
        s"create ${externalString}table $ifNotExistsString$tableName like parquet '${parquetFile.toString}' partitioned by ($partitionDef) stored as parquet location '$qualifiedPath'",
        s"alter table $tableName recover partitions"
      )
    }
  }
}

/**
  * Impala Database connector that is constructed using the Waimak JDBC template in spark conf
  *
  * @param context          The flow context object containing the SparkSession and FileSystem
  * @param database         name of the database to connect to
  * @param cluster          the cluster label in the JDBC template string
  * @param properties       Key value pairs passed as connection arguments to the DriverManager during connection
  * @param secureProperties Key value set of parameters used to get parameter values for JDBC properties
  *                         from a secure jceks file at CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH.
  *                         First value is the key of the parameter in the jceks file and the second parameter
  *                         is the key of the parameter you want in jdbc properties
  *
  */
case class ImpalaWaimakJDBCConnector(context: SparkFlowContext,
                                     database: String,
                                     cluster: String = "default",
                                     properties: java.util.Properties = new java.util.Properties(),
                                     secureProperties: Map[String, String] = Map.empty) extends ImpalaDBConnector with WaimakJDBCConnector {
  override val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  override val sparkConf: SparkConf = context.spark.sparkContext.getConf
  override val service: String = "impala"

  override def hadoopConfiguration: Configuration = context.spark.sparkContext.hadoopConfiguration
}

/**
  * Impala Database connector that is constructed from a JDBC connection string
  *
  * @param context          The flow context object containing the SparkSession and FileSystem
  * @param jdbcString       the JDBC connection string
  * @param properties       Key value pairs passed as connection arguments to the DriverManager during connection
  * @param secureProperties Key value set of parameters used to get parameter values for JDBC properties
  *                         from a secure jceks file at CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH.
  *                         First value is the key of the parameter in the jceks file and the second parameter
  *                         is the key of the parameter you want in jdbc properties
  */
case class ImpalaJDBCConnector(context: SparkFlowContext,
                               jdbcString: String,
                               properties: java.util.Properties = new java.util.Properties(),
                               secureProperties: Map[String, String] = Map.empty) extends ImpalaDBConnector with JDBCConnector {
  override val driverName: String = "org.apache.hive.jdbc.HiveDriver"

  override def hadoopConfiguration: Configuration = context.spark.sparkContext.hadoopConfiguration
}

/**
  * A dummy Impala database connector that does not submit the DDLs but
  * collects all that have been submitted in a List.
  * This is useful for testing or using in flows where you wish to collect
  * the DDLs and run them manually.
  *
  * @param context The flow context object containing the SparkSession and FileSystem
  */
case class ImpalaDummyConnector(context: SparkFlowContext) extends ImpalaDBConnector {
  var ranDDLs: List[List[String]] = List.empty

  override private[metastore] def runQueries(ddls: Seq[String]): Seq[Option[ResultSet]] = {
    ranDDLs = ranDDLs :+ ddls.toList
    Seq(None)
  }

}