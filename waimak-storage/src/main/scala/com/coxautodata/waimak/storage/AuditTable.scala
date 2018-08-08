package com.coxautodata.waimak.storage

import java.sql.Timestamp
import java.time.Duration

import org.apache.spark.sql.{Column, Dataset}

import scala.util.Try


/**
  * Main abstraction for an audit table that a client application must use to store records with a timestamp. It hides
  * all details of the physical storage, so that client apps can use various file systems (Ex: HDFS, ADLS, S3, Local, etc)
  * or key value (Ex: HBase).
  *
  * Also this abstraction can produce a snapshot of data de-duplicated on the primary key and true to the specified
  * moment in time.
  *
  * Also surfaces custom attributes initialised during table creation, so that client applications do not need to
  * worry about storing the relevant metadata in a separate storage. It also will simplify backup, restore and sharing
  * of data between environments.
  *
  * Some storage layers might be quite inefficient when it comes to storing lots of appends in multiple files and
  * storage optimisation, aka compaction, should not intervene with normal operation of the application. Therefore
  * application should be able to control when compaction can take place.
  *
  * An instance of AuditTable represents a functional state, if data was modified, do not use it again.
  *
  * There are 2 types of operations on the table:
  *   1. data extraction - which do not modify the state of the table, thus same instance of the AuditTable can be used
  * for multiple data extraction operations;
  *   2. data mutators - adding data to the table, optimising storage. These lead to new state of the underlying storage
  * and the same instance of AuditTable can not be used for data mutators again.
  *
  * Created by Alexei Perelighin on 2018/03/03
  */
trait AuditTable {

  /**
    * Custom attributes assigned by the client application during table creation.
    *
    * @return
    */
  def meta: Map[String, String]

  def regions: Seq[AuditTableRegionInfo]

  /**
    * Initializes audit table in the storage layer. It will also persist all of the metadata (name, primary keys, custom meta)
    * to the storage layer.
    *
    * @return new state of the table or error
    */
  def initNewTable(): Try[AuditTable]

  /**
    * Appends a new set of records to the audit table.
    *
    * Fails when is called second time on same instance.
    *
    * @param ds          records to append
    * @param lastUpdated column that returns java.sql.Timestamp that will be used for de-duplication on the primary keys
    * @param appendTS    timestamp of when the append has happened. It will not be used for de-duplications
    * @return (new state of the AuditTable, count of appended records) or error
    */
  def append(ds: Dataset[_], lastUpdated: Column, appendTS: Timestamp): Try[(AuditTable, Long)]

  /**
    * Generates snapshot that contains only the latest records for the given timestamp. De-duplication happens on the
    * primary keys.
    *
    * @param ts use records that are closest to this timestamp
    * @return if no data in storage layer, return None
    */
  def snapshot(ts: Timestamp): Option[Dataset[_]]

  /**
    * Include all records between the given timestamps.
    *
    * @param from
    * @param to
    * @return if no data in storage layer, return None
    */
  def allBetween(from: Option[Timestamp], to: Option[Timestamp]): Option[Dataset[_]]

  /**
    * Request optimisation of the storage layer.
    *
    * Fails when is called second time on same instance.
    *
    * @param compactTS   timestamp of when the compaction is requested, will not be used for any filtering of the data
    * @param trashMaxAge Maximum age of old region files kept in the .Trash folder
    *                    after a compaction has happened.
    * @return new state of the AuditTable
    */
  def compact(compactTS: Timestamp, trashMaxAge: Duration): Try[AuditTable]

  /**
    * Returns latest timestamp of records stored in the audit table.
    *
    * @return
    */
  def getLatestTimestamp(): Option[Timestamp]

  /**
    * Name of the table.
    *
    * @return
    */
  def tableName: String

}