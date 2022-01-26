package com.coxautodata.waimak.rdbm.ingestion

import com.coxautodata.waimak.configuration.CaseClassConfigParser

import java.sql.Timestamp
import java.util.Properties

/**
  * A mechanism for generating Waimak actions to extract data from a SQL Server instance
  *
  */
abstract class SQLServerBaseExtractor(override val connectionDetails: SQLServerConnectionDetails,
                                      override val extraConnectionProperties: Properties,
                                      val checkLastUpdatedTimestampRange: Boolean = false) extends RDBMExtractor {

  override val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

  override val sourceDBSystemTimestampFunction = "CURRENT_TIMESTAMP"

  val datetimeLowerTimestamp = Timestamp.valueOf("1900-01-01 00:00:00.0")

  def timestampDataTypeQuery(tableName: String, columnName: String): String =
    s"(select DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '${tableName}' and COLUMN_NAME = '${columnName}') m"

  def inferTimestampColDataType(label: String, lastUpdated: String) = {
    val columnType = RDBMIngestionUtils.lowerCaseAll(
      sparkSession.read
        .option("driver", driverClass)
        .jdbc(connectionDetails.jdbcString, timestampDataTypeQuery(label, lastUpdated), connectionProperties)
    )

    import columnType.sparkSession.implicits._

    columnType.select("data_type").as[String].first()
  }

  override def escapeKeyword(keyword: String) = if (escapeGuard(keyword)) s"[$keyword]" else keyword

  override def constrainLastUpdatedTimestampRange(timestamp: Timestamp, label: String, meta: Map[String, String]): Timestamp = {
    val tableMetadata = CaseClassConfigParser.fromMap[TableExtractionMetadata](meta)

    (checkLastUpdatedTimestampRange, tableMetadata.lastUpdatedColumn) match {
      case (true, Some(ts)) => mapLastUpdatedIntoAvailableRange(timestamp, label, ts)
      case _ => timestamp
    }
  }

  private def mapLastUpdatedIntoAvailableRange(timestamp: Timestamp, label: String, lastUpdated: String): Timestamp = {
    inferTimestampColDataType(label, lastUpdated) match {
      case "datetime" => if (timestamp.before(datetimeLowerTimestamp)) {
        logWarning(s"Timestamp out of range for datetime column $lastUpdated detected, using $datetimeLowerTimestamp instead")
        datetimeLowerTimestamp
      } else timestamp
      case "datetime2" => timestamp
      case _ @ colType =>
        logWarning(s"Last updated timestamp in the database has type $colType for table $label which may be incorrect")
        timestamp
    }
  }

  private def escapeGuard(keyword: String): Boolean = !(keyword.contains("[") && keyword.contains("]"))
}

/**
  * Connection details for a SQL Server instance
  *
  * @param server       the SQL Server instance
  * @param port         the port
  * @param databaseName the database name
  * @param user         the user name
  * @param password     the password
  */
case class SQLServerConnectionDetails(server: String
                                      , port: Int
                                      , databaseName: String
                                      , user: String
                                      , password: String) extends RDBMConnectionDetails {
  override val jdbcString: String = s"jdbc:sqlserver://$server:$port;databaseName=$databaseName;"
}
