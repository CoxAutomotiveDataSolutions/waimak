package com.coxautodata.waimak.rdbm.ingestion

import java.util.Properties

/**
  * A mechanism for generating Waimak actions to extract data from a SQL Server instance
  *
  */
abstract class SQLServerBaseExtractor(override val connectionDetails: SQLServerConnectionDetails,
                                      override val extraConnectionProperties: Properties) extends RDBMExtractor {

  override val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

  override val sourceDBSystemTimestampFunction = "CURRENT_TIMESTAMP"

  override def escapeKeyword(keyword: String) = if (escapeGuard(keyword)) s"[$keyword]" else keyword

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
