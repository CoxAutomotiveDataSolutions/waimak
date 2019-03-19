package com.coxautodata.waimak.metastore

import java.sql.{Connection, DriverManager, ResultSet}

import com.coxautodata.waimak.dataflow.DataFlowException
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH
import org.apache.spark.SparkConf

/**
  * Base trait that describes a basic DB connection allowing DDLs to be submitted
  */
trait DBConnector extends Logging {

  private[metastore] def runQuery(ddl: String): Option[ResultSet] = {
    runQueries(Seq(ddl)).head
  }

  private[metastore] def runQueries(ddls: Seq[String]): Seq[Option[ResultSet]]

  /**
    * Submit a query that returns no results (i.e. schema change operations)
    * Exceptions will be thrown if the query fails
    *
    * @param ddl SQL ddl as a string
    */
  def submitResultlessQuery(ddl: String): Unit = {
    runQuery(ddl)
  }

  def submitAtomicResultlessQueries(ddls: Seq[String]): Unit = {
    runQueries(ddls)
  }
}

/**
  * Trait that extends a JDBCConnector using templating on a JDBC string stored in the Spark config
  */
trait WaimakJDBCConnector extends JDBCConnector {

  def sparkConf: SparkConf

  def cluster: String

  def database: String

  def service: String

  def jdbcString: String = {
    sparkConf.getOption(s"spark.$service.$cluster.jdbc.template")
      .map(_.replaceFirst("DB_NAME", database))
      .getOrElse(throw new DataFlowException(s"JDBC action for cluster $cluster and service $service can not be created, no jdbc template"))
  }
}

/**
  * JDBC Database connection trait, that submits queries via a JDBC connection
  */
trait JDBCConnector extends DBConnector {

  /**
    * JDBC driver class to instantiate
    */
  def driverName: String

  /**
    * JDBC string for the connection
    */
  def jdbcString: String

  /**
    * Key value pairs passed as connection arguments to the DriverManager during connection
    */
  def properties: java.util.Properties

  /**
    * Key value set of parameters used to get parameter values for JDBC properties
    * from a secure jceks file at CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH.
    * First value is the key of the parameter in the jceks file and the second parameter
    * is the key of the parameter you want in jdbc properties
    *
    * @return
    */
  def secureProperties: Map[String, String]

  /**
    * Hadoop configuration object. Used to access secure credentials.
    */
  def hadoopConfiguration: Configuration

  private[metastore] def getAllProperties: java.util.Properties = {
    // Construct new properties and copy over existing to maintain immutability
    val newProps = new java.util.Properties()
    newProps.putAll(properties)

    secureProperties.foldLeft(newProps) {
      case (props, (jcekKey, jdbcKey)) =>
        setSecureProperty(jcekKey, jdbcKey, props)
        props
    }
  }

  private def setSecureProperty(jceksKey: String, jdbcKey: String, props: java.util.Properties): Unit = {
    Option(hadoopConfiguration.getPassword(jceksKey)) match {
      case Some(cred) =>
        props.setProperty(jdbcKey, cred.mkString)
      case None if hadoopConfiguration.get(CREDENTIAL_PROVIDER_PATH) == null =>
        throw new MetastoreUtilsException(s"Could not read secure parameter [$jceksKey] as no jceks file is set using [$CREDENTIAL_PROVIDER_PATH]")
      case None =>
        throw new MetastoreUtilsException(s"Could not find secure parameter [$jceksKey] in any locations at [${hadoopConfiguration.get(CREDENTIAL_PROVIDER_PATH)}]")
    }
  }

  private def getConnection: Connection = {
    Class.forName(driverName)
    DriverManager.getConnection(jdbcString, getAllProperties)
  }

  override private[metastore] def runQueries(ddls: Seq[String]): Seq[Option[ResultSet]] = {
    val statement = getConnection.createStatement
    ddls.map(ddl => {
      logInfo(s"Submitting query to $jdbcString: $ddl")
      val submit = statement.execute(ddl)
      if (submit) Some(statement.getResultSet)
      else None
    })
  }
}

/**
  * Hadoop database connection trait that has Hadoop-specific
  * table functions (i.e. create parquet tables)
  */
trait HadoopDBConnector extends DBConnector {

  import HadoopDBConnector._

  def context: SparkFlowContext

  def getPathsAndPartitionsForTables(tables: Seq[String]): Map[String, TablePathAndPartitions]

  /**
    * Force drop+create of tables even if update is called (necessary in cases of schema change)
    */
  def forceRecreateTables: Boolean = context.getBoolean(FORCE_RECREATE_TABLES, FORCE_RECREATE_TABLES_DEFAULT)

  private[metastore] def createTableFromParquetDDL(tableName: String, path: String, external: Boolean = true, partitionColumns: Seq[String] = Seq.empty, ifNotExists: Boolean = true): Seq[String]

  private[metastore] def dropTableParquetDDL(tableName: String, ifExistsOption: Boolean = true): String = {
    s"drop table" + (if (ifExistsOption) " if exists " else " ") + tableName
  }

  private[metastore] def updateTableLocationDDL(tableName: String, path: String): String = {
    s"alter table $tableName set location '$path'"
  }

  /**
    * Recreate a table from parquet files, inferring the schema from the parquet.
    * Table is dropped if it exists and then recreated.
    *
    * @param tableName        name of the table
    * @param path             path of the table location
    * @param partitionColumns optional list of partition columns
    * @return the sql statements which need executing to perform the table recreation
    */
  def recreateTableFromParquetDDLs(tableName: String, path: String, partitionColumns: Seq[String] = Seq.empty): Seq[String] = {
    dropTableParquetDDL(tableName) +: createTableFromParquetDDL(tableName, path, partitionColumns = partitionColumns)
  }

  /**
    * Update the data location of a parquet table.
    * If the table is partitioned, it will be dropped if it exists and recreated.
    * If the table is not partitioned and forceRecreateTables is true, it will be dropped if it exists and recreated.
    * If the table is not partitioned and forceRecreateTables is false and the schema has not changed, then the data location will be changed without dropping the table.
    * The table will be created if it does not already exist.
    *
    * @param tableName        name of the table
    * @param path             path of the table location
    * @param partitionColumns optional list of partition columns
    * @return the sql statements which need executing to perform the table update
    */
  def updateTableParquetLocationDDLs(tableName: String, path: String, partitionColumns: Seq[String] = Seq.empty, schemaChanged: Boolean = false): Seq[String] = {
    {
      if (partitionColumns.nonEmpty || forceRecreateTables || schemaChanged) recreateTableFromParquetDDLs(tableName, path, partitionColumns)
      else createTableFromParquetDDL(tableName, path) :+ updateTableLocationDDL(tableName, path)
    }
  }
}

object HadoopDBConnector {
  val metastoreParamPrefix: String = "spark.waimak.metastore"
  /**
    * Force recreate (drop+create) of all tables submitted through connector objects.
    * This should be done in case of underlying schema changes to data files.
    * Default: false
    */
  val FORCE_RECREATE_TABLES: String = s"$metastoreParamPrefix.forceRecreateTables"
  val FORCE_RECREATE_TABLES_DEFAULT: Boolean = false
  /**
    * Whether to auto-detect schema changes to tables and automatically recreate the
    * tables where necessary.
    * Default: true
    */
  val AUTODETECT_SCHEMA_CHANGES: String = s"$metastoreParamPrefix.autodetectSchemaChanges"
  val AUTODETECT_SCHEMA_CHANGES_DEFAULT: Boolean = true
}

private[waimak] case class TablePathAndPartitions(path: Option[Path], partitions: Seq[String])