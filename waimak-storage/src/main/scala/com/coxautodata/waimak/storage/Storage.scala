package com.coxautodata.waimak.storage

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Contains methods to create tables, open tables.
  *
  * Created by Alexei Perelighin on 2018/04/11
  */
object Storage {

  /**
    * Creates file table with default configurations, region ids will Longs with zeros added to the left to make it
    * 20 chars.
    *
    * @param sparkSession
    * @param basePath  parent folder which contains folders with table names
    * @param tableInfo table metadata
    * @return
    * @throws  StorageException : Storage exceptions when:
    *                           1) primary keys are not specified;
    *                           2) the folder already exists
    */
  def createFileTable(sparkSession: SparkSession, basePath: Path
                      , tableInfo: AuditTableInfo): Try[AuditTable] = {
    val fileStorageOps = createFops(sparkSession, basePath)
    val table = AuditTableFile.createTable(sparkSession, fileStorageOps, basePath, tableInfo)(AuditTableFile.nextLongRegion)
    table
  }

  /**
    * Opens multiple tables, reads their metadata and reads the state of the table regions (ids, count of records per region,
    * values of region latest timestamp).
    *
    * You can specify to only read the cold (compacted) state of the table. This is useful to avoid consistency issues during development on tables into which data is currently flowing.
    * This is useful to avoid consistency issues during development on tables into which data is currently flowing.
    *
    * @param sparkSession
    * @param basePath   parent folder which contains folders with table names
    * @param tableNames list of tables to open
    * @param includeHot include freshly appended data that has not been compacted yet. Useful for using production data in development.
    * @return (Map[TABLE NAME, AuditTable], Seq[MISSING TABLES]) - audit table objects that exist and
    *         of table names that were not found under the basePath
    */
  def openFileTables(sparkSession: SparkSession, basePath: Path, tableNames: Seq[String], includeHot: Boolean = true): (Map[String, Try[AuditTable]], Seq[String]) = {
    val fileStorageOps = createFops(sparkSession, basePath)
    val res = AuditTableFile.openTables(sparkSession, fileStorageOps, basePath, tableNames, includeHot)(AuditTableFile.nextLongRegion)
    res
  }

  /**
    * Creates File Operations object that is a bridge between the process actions and actual storage and handle
    * write to temp with move to permanent operations.
    *
    * @param sparkSession
    * @param basePath parent folder which contains folders with table names, .tmp and .Trash folders will be underneath.
    * @return
    */
  def createFops(sparkSession: SparkSession, basePath: Path): FileStorageOps = new FileStorageOpsWithStaging(
    FileSystem.get(basePath.toUri(), sparkSession.sparkContext.hadoopConfiguration)
    , sparkSession
    , new Path(basePath, ".tmp")
    , new Path(basePath, ".Trash")
  )
}
