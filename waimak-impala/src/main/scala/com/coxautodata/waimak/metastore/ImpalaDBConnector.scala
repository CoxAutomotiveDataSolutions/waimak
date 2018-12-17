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

  def fileSystem: FileSystem

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
  * @param sparkFlowContext    The flow context object containing the SparkSession and FileSystem
  * @param database            name of the database to connect to
  * @param cluster             the cluster label in the JDBC template string
  * @param forceRecreateTables Force drop+create of tables even if update is called (necessary in cases of schema change)
  * @param properties          Key value pairs passed as connection arguments to the DriverManager during connection
  * @param secureProperties    Key value set of parameters used to get parameter values for JDBC properties
  *                            from a secure jceks file at CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH.
  *                            First value is the key of the parameter in the jceks file and the second parameter
  *                            is the key of the parameter you want in jdbc properties
  *
  */
case class ImpalaWaimakJDBCConnector(sparkFlowContext: SparkFlowContext,
                                     database: String,
                                     cluster: String = "default",
                                     forceRecreateTables: Boolean = false,
                                     properties: java.util.Properties = new java.util.Properties(),
                                     secureProperties: Map[String, String] = Map.empty) extends ImpalaDBConnector with WaimakJDBCConnector {
  override val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  override val sparkConf: SparkConf = sparkSession.sparkContext.getConf
  override val service: String = "impala"

  override def sparkSession: SparkSession = sparkFlowContext.spark

  override def fileSystem: FileSystem = sparkFlowContext.fileSystem

  override def hadoopConfiguration: Configuration = sparkSession.sparkContext.hadoopConfiguration
}

/**
  * Impala Database connector that is constructed from a JDBC connection string
  *
  * @param sparkFlowContext    The flow context object containing the SparkSession and FileSystem
  * @param jdbcString          the JDBC connection string
  * @param forceRecreateTables Force drop+create of tables even if update is called (necessary in cases of schema change)
  * @param properties          Key value pairs passed as connection arguments to the DriverManager during connection
  * @param secureProperties    Key value set of parameters used to get parameter values for JDBC properties
  *                            from a secure jceks file at CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH.
  *                            First value is the key of the parameter in the jceks file and the second parameter
  *                            is the key of the parameter you want in jdbc properties
  */
case class ImpalaJDBCConnector(sparkFlowContext: SparkFlowContext,
                               jdbcString: String,
                               forceRecreateTables: Boolean = false,
                               properties: java.util.Properties = new java.util.Properties(),
                               secureProperties: Map[String, String] = Map.empty) extends ImpalaDBConnector with JDBCConnector {
  override val driverName: String = "org.apache.hive.jdbc.HiveDriver"

  override def sparkSession: SparkSession = sparkFlowContext.spark

  override def fileSystem: FileSystem = sparkFlowContext.fileSystem

  override def hadoopConfiguration: Configuration = sparkSession.sparkContext.hadoopConfiguration
}

/**
  * A dummy Impala database connector that does not submit the DDLs but
  * collects all that have been submitted in a List.
  * This is useful for testing or using in flows where you wish to collect
  * the DDLs and run them manually.
  *
  * @param sparkFlowContext    The flow context object containing the SparkSession and FileSystem
  * @param forceRecreateTables Force drop+create of tables even if update is called (necessary in cases of schema change)
  */
case class ImpalaDummyConnector(sparkFlowContext: SparkFlowContext, forceRecreateTables: Boolean = false) extends ImpalaDBConnector {
  var ranDDLs: List[List[String]] = List.empty

  override private[metastore] def runQueries(ddls: Seq[String]): Seq[Option[ResultSet]] = {
    ranDDLs = ranDDLs :+ ddls.toList
    Seq(None)
  }

  override def sparkSession: SparkSession = sparkFlowContext.spark

  override def fileSystem: FileSystem = sparkFlowContext.fileSystem

}