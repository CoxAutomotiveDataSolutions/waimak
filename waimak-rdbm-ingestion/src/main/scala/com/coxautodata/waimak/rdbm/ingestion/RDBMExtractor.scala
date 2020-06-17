package com.coxautodata.waimak.rdbm.ingestion

import java.sql.Timestamp
import java.util.Properties

import com.coxautodata.waimak.configuration.CaseClassConfigParser
import com.coxautodata.waimak.log.Level.{Debug, Info}
import com.coxautodata.waimak.log.{Level, Logging}
import com.coxautodata.waimak.storage.AuditTableInfo
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import scala.util.Try

/**
  * Waimak RDBM connection mechanism
  */
trait RDBMExtractor extends Logging {

  def connectionDetails: RDBMConnectionDetails

  /**
    * Escape a keyword (for use in a query)
    * e.g. SQlServer uses [], Postgres uses ""
    *
    * @param keyword the keyword to escape
    * @return the escaped keyword
    */
  def escapeKeyword(keyword: String): String

  /**
    * The JDBC driver to use for this RDBM
    */
  def driverClass: String

  def sparkSession: SparkSession

  /**
    * The function to use to get the system timestamp in the database
    */
  def sourceDBSystemTimestampFunction: String

  /**
    * This is what the column containing the system timestamp will be called in the output DataFrames
    */
  def systemTimestampColumnName: String = "system_timestamp_of_extraction"

  /**
    * This is what the column to use as the last updated in the output DataFrames will be called
    * (In some cases, this will come from the provided last updated column, in others it will be
    * the system timestamp)
    */
  def rdbmRecordLastUpdatedColumn: String = "rdbm_record_last_updated"

  /**
    * How to transform the target table name into the table name in the database if the two are different.
    * Useful if you have multiple tables representing the same thing but with different names, and you wish them to
    * be written to the same target table
    *
    * @return a function which takes a target table name and returns the table name in the database
    */
  def transformTableNameForRead: String => String = identity

  /**
    * Tries to get whatever metadata information it can from the database
    * Uses the optional provided values for pks and lastUpdated if it cannot get them from the database.
    *
    * @param dbSchemaName              the database schema name
    * @param tableName                 the table name
    * @param primaryKeys               Optionally, the primary keys for this table (not needed if this extractor can discover
    *                                  primary keys itself)
    * @param lastUpdatedColumn         Optionally, the last updated column for this table (not needed if this extractor
    *                                  can discover last updated columns itself). If this is undefined and this extractor
    *                                  does not discover a last updated column for the table, then this table will be extracted
    *                                  in full every time
    * @param forceRetainStorageHistory Optionally specify whether to retain history for this table in the storage layer.
    *                                  Setting this to anything other than None will override the default behaviour which is:
    *                                  - if there is a lastUpdated column found or specified, retain all history for this table
    *                                  - if there is no lastUpdated column, don't retain history for this table (history is
    *                                  removed when the table is compacted). The choice of this default behaviour is because,
    *                                  without a lastUpdatedColumn, the table will be extracted in full every time extraction
    *                                  is performed, causing the size of the data in storage to grow uncontrollably
    * @return Success[AuditTableInfo] if all required metadata was either found or provided by the user
    *         Failure if required metadata was neither found nor provided by the user
    *         Failure if metadata provided differed from the metadata found in the database
    */
  def getTableMetadata(dbSchemaName: String
                       , tableName: String
                       , primaryKeys: Option[Seq[String]]
                       , lastUpdatedColumn: Option[String]
                       , forceRetainStorageHistory: Option[Boolean]): Try[AuditTableInfo] = {
    getTableMetadata(dbSchemaName
      , tableName
      , primaryKeys
      , lastUpdatedColumn
      , resolvedLastUpdatedColumn => forceRetainStorageHistory.getOrElse(resolvedLastUpdatedColumn.isDefined))
  }

  /**
    * Subclasses of [[RDBMExtractor]] must implement this method which:
    * - tries to get whatever metadata information it can from the database
    * - uses the optional provided values for pks and lastUpdated if it cannot get them from the database
    *
    * This differs from the method defined above in the retainStorageHistory parameter. It takes a function which, given
    * an optional lastUpdated column, returns whether or not to retain storage history for this table. Implementations
    * should call this function to get the value needed by the [[AuditTableInfo]]
    */
  protected def getTableMetadata(dbSchemaName: String
                                 , tableName: String
                                 , primaryKeys: Option[Seq[String]]
                                 , lastUpdatedColumn: Option[String]
                                 , retainStorageHistory: Option[String] => Boolean): Try[AuditTableInfo]

  def resolveLastUpdatedColumn(tableMetadata: ExtractionMetadata, sparkSession: SparkSession): Column = {
    import sparkSession.implicits._
    $"${tableMetadata.lastUpdatedColumn.getOrElse(systemTimestampColumnName)}"
  }

  /**
    * JDBC connection properties
    */
  def extraConnectionProperties: Properties


  protected def connectionProperties: Properties = {
    extraConnectionProperties.setProperty("user", connectionDetails.user)
    extraConnectionProperties.setProperty("password", connectionDetails.password)
    extraConnectionProperties
  }

  /**
    * Creates a Dataset for the given table containing data which was updated after or on the provided timestamp
    * Override this if required, or if you wish to use a different metadata class than TableExtractionMetadata
    *
    * @param meta                the table metadata
    * @param lastUpdated         the last updated timestamp from which we wish to read data (if None, then we read everything)
    * @param maxRowsPerPartition Optionally, the maximum number of rows to be read per Dataset partition for this table
    *                            This number will be used to generate predicates to be passed to org.apache.spark.sql.SparkSession.read.jdbc
    *                            If this is not set, the DataFrame will only have one partition. This could result in memory
    *                            issues when extracting large tables.
    *                            Be careful not to create too many partitions in parallel on a large cluster; otherwise
    *                            Spark might crash your external database systems. You can also control the maximum number
    *                            of jdbc connections to open by limiting the number of executors for your application.
    * @return (Dataset for the given table, Column to use as the last updated)
    */
  protected def loadDataset(meta: Map[String, String]
                            , lastUpdated: Option[Timestamp]
                            , maxRowsPerPartition: Option[Int]): (Dataset[_], Column) = {
    val tableMetadata = CaseClassConfigParser.fromMap[TableExtractionMetadata](meta)
    (sparkLoad(tableMetadata, lastUpdated, maxRowsPerPartition), resolveLastUpdatedColumn(tableMetadata, sparkSession))
  }

  /**
    * Creates a Dataset for the given table containing data which was updated after or on the provided timestamp
    *
    * @param meta                the table metadata
    * @param lastUpdated         the last updated for the table (if None, then we read everything)
    * @param maxRowsPerPartition Optionally, the maximum number of rows to be read per Dataset partition for this table
    *                            This number will be used to generate predicates to be passed to org.apache.spark.sql.SparkSession.read.jdbc
    *                            If this is not set, the DataFrame will only have one partition. This could result in memory
    *                            issues when extracting large tables.
    *                            Be careful not to create too many partitions in parallel on a large cluster; otherwise
    *                            Spark might crash your external database systems. You can also control the maximum number
    *                            of jdbc connections to open by limiting the number of executors for your application.
    * @param forceFullLoad       If set to true, ignore the last updated and read everything
    * @return a Dataset for the given table
    */
  final def getTableDataset(meta: Map[String, String]
                            , lastUpdated: Option[Timestamp]
                            , maxRowsPerPartition: Option[Int] = None
                            , forceFullLoad: Boolean = false): Dataset[_] = {
    val (dataSet, columnToUseAsLastUpdated) = loadDataset(meta, lastUpdated.filter(_ => !forceFullLoad), maxRowsPerPartition)
    dataSet.withColumn(rdbmRecordLastUpdatedColumn, columnToUseAsLastUpdated)
  }


  /**
    * Generate a query to select from the given table
    *
    * @param tableMetadata         the metadata for the table
    * @param lastUpdated           the last updated timestamp from which we wish to read data
    * @param explicitColumnSelects any additional columns which need to be specified on read (which won't be picked up by
    *                              select *) e.g. HIDDEN fields
    * @return a query which selects from the given table
    */
  def selectQuery(tableMetadata: ExtractionMetadata
                  , lastUpdated: Option[Timestamp]
                  , explicitColumnSelects: Seq[String]): String = {
    val extraSelectCols = (explicitColumnSelects :+ s"$sourceDBSystemTimestampFunction as $systemTimestampColumnName").mkString(",")
    logAndReturn(
      s"""(select *, $extraSelectCols ${fromQueryPart(tableMetadata, lastUpdated)}) s""",
      (query: String) => s"Query: $query for metadata ${tableMetadata.toString} for lastUpdated ${lastUpdated}",
      Info
    )
  }

  protected def fromQueryPart(tableMetadata: ExtractionMetadata
                              , lastUpdated: Option[Timestamp]): String = {
    (tableMetadata.lastUpdatedColumn, lastUpdated) match {
      case (Some(lastUpdatedCol), Some(ts)) =>
        s"from ${tableMetadata.qualifiedTableName(escapeKeyword)} where ${escapeKeyword(lastUpdatedCol)} > '$ts'"
      case _ => s"from ${tableMetadata.qualifiedTableName(escapeKeyword)}"
    }
  }

  /**
    * Creates a Spark Dataset for the table
    *
    * @return a Spark Dataset for the table
    */
  def sparkLoad(tableMetadata: ExtractionMetadata
                , lastUpdated: Option[Timestamp]
                , maxRowsPerPartition: Option[Int]
                , explicitColumnSelects: Seq[String] = Seq.empty): Dataset[_] = {
    val actualTableMetadata: ExtractionMetadata = tableMetadata.transformTableName(transformTableNameForRead)
    val select = selectQuery(actualTableMetadata, lastUpdated, explicitColumnSelects)
    maxRowsPerPartition.flatMap(maxRows => generateSplitPredicates(actualTableMetadata, lastUpdated, maxRows))
      .map(predicates => {
        sparkSession.read
          .option("driver", driverClass)
          .jdbc(connectionDetails.jdbcString, select, predicates, connectionProperties)
      }).getOrElse(
      sparkSession.read
        .option("driver", driverClass)
        .jdbc(connectionDetails.jdbcString, select, connectionProperties)
    )
  }

  /**
    * Generates predicates which are used to form the partitions of the read Dataset
    * Queries the table to work out the primary key boundary points to use
    * (so that each partition will contain a maximum of maxRowsPerPartition rows)
    *
    * @param tableMetadata       the table metadata
    * @param lastUpdated         the last updated timestamp from which we wish to read data
    * @param maxRowsPerPartition the maximum number of rows we want in each partition
    * @return If the Dataset will have fewer rows than maxRowsPerParition then None, otherwise predicates to use
    *         in order to create the partitions e.g. "id >= 5 and id < 7"
    */
  def generateSplitPredicates(tableMetadata: ExtractionMetadata
                              , lastUpdated: Option[Timestamp]
                              , maxRowsPerPartition: Int): Option[Array[String]] = {
    val spark = sparkSession
    import spark.implicits._

    val splitPoints = sparkSession.read
      .option("driver", driverClass)
      .jdbc(connectionDetails.jdbcString, splitPointsQuery(tableMetadata, lastUpdated, maxRowsPerPartition), connectionProperties)
      .as[String]
      .collect().toList

    splitPointsToPredicates(splitPoints, tableMetadata)
  }

  private[rdbm] def splitPointCol(tableMetadata: ExtractionMetadata) =
    logAndReturn((if (tableMetadata.pkCols.tail.nonEmpty) s"CONCAT(${tableMetadata.pkCols.map(escapeKeyword).mkString(",'-',")})"
    else escapeKeyword(tableMetadata.pkCols.head)),
      (col: String) => s"Split point col: $col",
      Level.Info)

  //TODO: Need to look into ordering of primary keys as clustered index could have different ordering
  private[rdbm] def splitPointsQuery(tableMetadata: ExtractionMetadata
                                     , lastUpdated: Option[Timestamp]
                                     , maxRowsPerPartition: Int): String = {
    s"""(
       |select split_point from (
       |select ${splitPointCol(tableMetadata)} as split_point, row_number() over (order by ${tableMetadata.pkCols.map(escapeKeyword).mkString(",")}) as _row_num
       |${fromQueryPart(tableMetadata, lastUpdated)}
       |) ids where _row_num % $maxRowsPerPartition = 0) s""".stripMargin
  }

  //TODO: For composite keys, we can use first pk col in where clause as well as the concat to avoid full scans
  private[rdbm] def splitPointsToPredicates(splitPoints: Seq[String], tableMetadata: ExtractionMetadata): Option[Array[String]] = {
    val splitPointColumn = splitPointCol(tableMetadata)
    if (splitPoints.nonEmpty) {
      val mainPredicates = if (splitPoints.tail.nonEmpty) {
        splitPoints.sliding(2).toList.map(arr => s"$splitPointColumn >= '${arr.head}' and $splitPointColumn < '${arr.tail.head}'")
      } else Seq.empty

      val endPointPredicates = Seq(
        s"$splitPointColumn < '${splitPoints.head}'"
        , s"$splitPointColumn >= '${splitPoints.reverse.head}'"
      )
      Some((mainPredicates ++ endPointPredicates).toArray)
    }
    else None
  }

}

case class IncorrectUserPKException(userPKs: Seq[String], dbPKs: Seq[String]) extends Exception(
  s"""
     | User-provided primary keys did not match those found in the database.
     | User provided: ${userPKs.mkString(",")}
     | From DB: ${dbPKs.mkString(",")}""".stripMargin)

object PKsNotFoundOrProvidedException extends Exception("PK cannot be found in the database so must be provided")