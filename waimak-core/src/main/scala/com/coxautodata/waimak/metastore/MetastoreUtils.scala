package com.coxautodata.waimak.metastore

import java.sql.{DriverManager, ResultSet}

import com.coxautodata.waimak.dataflow.DataFlowException
import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
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

  override private[metastore] def runQueries(ddls: Seq[String]): Seq[Option[ResultSet]] = {
    Class.forName(driverName)
    val connection = DriverManager.getConnection(jdbcString, "", "")
    val statement = connection.createStatement
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

  /**
    * Force drop+create of tables even if update is called (necessary in cases of schema change)
    */
  def forceRecreateTables: Boolean

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
    */
  def recreateTableFromParquet(tableName: String, path: String, partitionColumns: Seq[String] = Seq.empty): Unit = {
    submitAtomicResultlessQueries(dropTableParquetDDL(tableName) +: createTableFromParquetDDL(tableName, path, partitionColumns = partitionColumns))
  }

  /**
    * Update the data location of a parquet table.
    * If the table is partitioned, it will be dropped if it exists and recreated.
    * If the table is not partitioned and forceRecreateTables is true, it will be dropped if it exists and recreated.
    * If the table is not partitioned and forceRecreateTables is false, then the data location will be changed without dropping the table.
    * The table will be created if it does not already exist.
    *
    * @param tableName        name of the table
    * @param path             path of the table location
    * @param partitionColumns optional list of partition columns
    */
  def updateTableParquetLocation(tableName: String, path: String, partitionColumns: Seq[String] = Seq.empty): Unit = {
    {
      if (partitionColumns.nonEmpty || forceRecreateTables) recreateTableFromParquet(tableName, path, partitionColumns)
      else submitAtomicResultlessQueries(createTableFromParquetDDL(tableName, path) :+ updateTableLocationDDL(tableName, path))
    }
  }
}