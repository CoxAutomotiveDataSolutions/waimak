package com.coxautodata.waimak.rdbm.ingestion

import java.sql.Timestamp

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by Vicky Avison on 04/04/18.
  */
object RDBMIngestionUtils {

  def lowerCaseAll(df: Dataset[_]): Dataset[_] = {
    import df.sparkSession.implicits._
    val cols = df.schema.fieldNames.map(f => lower($"$f").as(f))
    df.select(cols: _*)
  }

  /**
    * Converts a case class to a Map
    *
    * This should probably go in CaseClassConfigParser but needs to cover all scenarios first
    *
    * @param instance
    * @tparam A
    * @return
    */
  def caseClassToMap[A](instance: A): Map[String, Any] = {

    def isNone(value: Any): Boolean = value match {
      case None => true
      case _ => false
    }

    def unpack(value: Any): Any = {
      value match {
        case Some(v) => unpack(v)
        case xs: Seq[Any] => xs.mkString(",")
        case _ => value
      }
    }

    instance.getClass.getDeclaredFields.map(f => {
      f.setAccessible(true)
      val value = f.get(instance)
      val unpacked = unpack(value)
      f.getName -> unpacked
    }).filterNot(kv => isNone(kv._2)).toMap
  }

  /**
    * For SQlServer temporal tables, using com.coxautodata.waimak.storage.AuditTable.snapshot(ts) to snapshot will not work due
    * to the extra complication of resolving the main and history tables to find delete events.
    * To correctly snapshot a temporal table, for each pk we need to:
    * 1. order by start col desc, end col asc
    * 2. take the first record in this ordering IF the snapshot timestamp is between the start col and the end col,
    * otherwise return no record for this pk (this record has been deleted)
    *
    * @param ds                    The dataset to snapshot
    * @param snapshotTimestamp     the snapshot timestamp
    * @param temporalTableMetadata the metadata for the temporal table
    * @return a snapshot of the dataset for the given snapshot timestamp
    * @throws RuntimeException if the table is not temporal
    */
  def snapshotTemporalTableDataset(ds: Dataset[_]
                                   , snapshotTimestamp: Timestamp
                                   , temporalTableMetadata: SQLServerTemporalTableMetadata): Dataset[_] = {
    import ds.sparkSession.implicits._
    if (!temporalTableMetadata.isTemporal) throw new RuntimeException("Cannot call this function with a non-temporal table")
    val window = Window.partitionBy(temporalTableMetadata.pkCols.map(ds(_)): _*)
      .orderBy(ds(temporalTableMetadata.startColName.get).desc, ds(temporalTableMetadata.endColName.get).asc)
    ds.filter($"${temporalTableMetadata.startColName.get}" <= snapshotTimestamp)
      //If the start and end cols collide, these can cause us to falsely assume that the record was deleted. If we
      //have to make assumptions due to event collisions, we would rather assume the record exists
      .filter($"${temporalTableMetadata.startColName.get}" =!= $"${temporalTableMetadata.endColName.get}")
      .withColumn("_row_num", row_number() over window)
      .filter($"_row_num" === 1
        && lit(snapshotTimestamp) >= $"${temporalTableMetadata.startColName.get}"
        && lit(snapshotTimestamp) < $"${temporalTableMetadata.endColName.get}")
      .drop("_row_num")
  }
}

/**
  * Table configuration used for RDBM extraction
  *
  * @param tableName                 The name of the table
  * @param pkCols                    Optionally, the primary key columns for this table (don't need if the implementation of
  *                                  [[RDBMExtractor]] is capable of getting this information itself)
  * @param lastUpdatedColumn         Optionally, the last updated column for this table (don't need if the implementation of
  *                                  [[RDBMExtractor]] is capable of getting this information itself)
  * @param maxRowsPerPartition       Optionally, the maximum number of rows to be read per Dataset partition for this table
  *                                  This number will be used to generate predicates to be passed to org.apache.spark.sql.SparkSession.read.jdbc
  *                                  If this is not set, the DataFrame will only have one partition. This could result in memory
  *                                  issues when extracting large tables.
  *                                  Be careful not to create too many partitions in parallel on a large cluster; otherwise
  *                                  Spark might crash your external database systems. You can also control the maximum number
  *                                  of jdbc connections to open by limiting the number of executors for your application.
  * @param forceRetainStorageHistory Optionally specify whether to retain history for this table in the storage layer.
  *                                  Setting this to anything other than None will override the default behaviour which is:
  *                                  - if there is a lastUpdated column (either specified here or found by the [[RDBMExtractor]])
  *                                  retain all history for this table
  *                                  - if there is no lastUpdated column, don't retain history for this table (history is
  *                                  removed when the table is compacted). The choice of this default behaviour is because,
  *                                  without a lastUpdatedColumn, the table will be extracted in full every time extraction
  *                                  is performed, causing the size of the data in storage to grow uncontrollably
  */
case class RDBMExtractionTableConfig(tableName: String
                                     , pkCols: Option[Seq[String]] = None
                                     , lastUpdatedColumn: Option[String] = None
                                     , maxRowsPerPartition: Option[Int] = None
                                     , forceRetainStorageHistory: Option[Boolean] = None)
