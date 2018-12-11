package com.coxautodata.waimak.storage

import java.sql.Timestamp
import java.time.Duration

import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

/**
  * Implementation of the [[AuditTable]] which is backed up by append only block storage like HDFS.
  *
  * Created by Alexei Perelighin on 2018/03/03
  *
  * @param tableInfo   static details about the table, with custom metadata
  * @param regions     list of region details
  * @param storageOps  object that actually interacts with the physical storage
  * @param baseFolder  parent folder which contains folders with table names
  * @param newRegionID function that generates region ids
  */
class AuditTableFile(val tableInfo: AuditTableInfo
                     , override val regions: Seq[AuditTableRegionInfo]
                     , val storageOps: FileStorageOps
                     , val baseFolder: Path
                     , val newRegionID: (AuditTableFile) => String
                    ) extends AuditTable with Logging {

  import AuditTableFile._

  /**
    * Not thread safe. Protection against using mutator functions more than one time.
    */
  protected var wasModified = false

  protected val tablePath = new Path(baseFolder, tableInfo.table_name)

  protected val regionInfoBasePath = new Path(baseFolder, AuditTableFile.REGION_INFO_DIRECTORY)

  protected val coldPath = new Path(tablePath, s"$STORE_TYPE_COLUMN=$COLD_PARTITION")

  protected val hotPath = new Path(tablePath, s"$STORE_TYPE_COLUMN=$HOT_PARTITION")

  protected val metaBasePath = new Path(tablePath, "_metadata")

  override def meta: Map[String, String] = tableInfo.meta

  override def tableName: String = tableInfo.table_name

  override def getLatestTimestamp(): Option[Timestamp] = if (regions.nonEmpty) Some(regions.map(_.max_last_updated).reduce((t1, t2) => if (t1.after(t2)) t1 else t2)) else None

  override def append(ds: Dataset[_], lastUpdated: Column, appendTS: Timestamp): Try[(AuditTable, Long)] = {
    val res: Try[(AuditTableFile.this.type, Long)] = Try {
      markToUpdate()
      val regionID = newRegionID(this)
      val regionPath = new Path(hotPath, s"$STORE_REGION_COLUMN=" + regionID)
      logInfo(s"Creating region in path [${regionPath.toString}]")
      val withLastUpdated = ds.withColumn(DE_LAST_UPDATED_COLUMN, lastUpdated)
      storageOps.writeParquet(tableInfo.table_name, regionPath, withLastUpdated)
      val (count, max_latest_ts) = calcRegionStats(storageOps.openParquet(regionPath).get)
      val region = AuditTableRegionInfo(tableInfo.table_name, HOT_PARTITION, regionID, appendTS, false, count, max_latest_ts)
      logInfo(s"Created region $region")
      (setRegions(this, regions :+ region).asInstanceOf[this.type], count)
    }
    res
  }

  override def snapshot(ts: Timestamp): Option[Dataset[_]] = {
    //TODO: optimise and explore other solutions with the use of counts, as smaller counts should avoid shuffle, hot applied to cold
    val auditRows = allBetween(None, Some(ts))
    auditRows.map { rows =>
      val primaryKeyColumns = tableInfo.primary_keys.map(rows(_))
      val windowLatest = Window.partitionBy(primaryKeyColumns: _*).orderBy(rows(DE_LAST_UPDATED_COLUMN).desc)
      rows.withColumn("_rowNum", row_number().over(windowLatest)).filter("_rowNum = 1").drop("_rowNum")
        .drop(DE_LAST_UPDATED_COLUMN)
    }
  }

  override def compact(compactTS: Timestamp
                       , trashMaxAge: Duration
                       , hotCellsPerPartition: Int = 10000000
                       , rowsPerRegion: Int = 50000000
                       , coldCellsPerPartition: Int = 25000000): Try[this.type] = {
    val res: Try[AuditTableFile] = Try(markToUpdate())
      .flatMap(_ => commitHotToCold(compactTS, hotCellsPerPartition))
      .flatMap(_.compactCold(compactTS, rowsPerRegion, coldCellsPerPartition))
      .map { f =>
        f.storageOps.purgeTrash(f.tableName, compactTS, trashMaxAge)
        f
      }
    res.map(_.asInstanceOf[this.type])
  }

  override def initNewTable(): Try[this.type] = {
    logInfo(s"Initialising table [${tableInfo.table_name}] in path [${tablePath.toString}]")
    val res: Try[this.type] = Try(true)
      .flatMap(_ => if (storageOps.pathExists(tablePath)) Failure(StorageException(s"Table [${tableInfo.table_name}] already exists in path [${tablePath.toString}]")) else Success(true))
      .flatMap(_ => if (tableInfo.primary_keys.isEmpty) Failure(StorageException(s"Table [${tableInfo.table_name}] must have at least one column in primary keys.")) else Success(true))
      .flatMap(_ => if (storageOps.mkdirs(hotPath)) Success(true) else Failure(StorageException(s"Table [${tableInfo.table_name}] can not be initialised, can not create folder ${hotPath.toString}")))
      .flatMap(_ => if (storageOps.mkdirs(coldPath)) Success(true) else Failure(StorageException(s"Table [${tableInfo.table_name}] can not be initialised, can not create folder ${coldPath.toString}")))
      .flatMap(_ => storageOps.writeAuditTableInfo(baseFolder, tableInfo))
      .map(_ => setRegions(this, Seq.empty).asInstanceOf[this.type])
    res
  }

  override def allBetween(from: Option[Timestamp], to: Option[Timestamp]): Option[Dataset[_]] = {
    val regionIDs = activeRegionIDs()
    regionIDs.flatMap { ids =>
      val df = storageOps.openParquet(tablePath)
      df.map { rows =>
        val auditRows = rows.filter(rows(STORE_TYPE_COLUMN).isin(HOT_PARTITION, COLD_PARTITION) && rows(STORE_REGION_COLUMN).isin(ids: _*))
        auditRows.filter(auditRows(DE_LAST_UPDATED_COLUMN).between(from.getOrElse(lowTimestamp), to.getOrElse(highTimestamp)))
      }
    }
  }

  /**
    * Returns regions ids for all active regions.
    *
    * @return
    */
  def activeRegionIDs(): Option[Seq[String]] = if (regions.isEmpty) None else Some(regions.filter(!_.is_deprecated).map(_.store_region))

  /**
    * Compacts all hot regions into one cold.
    *
    * @param compactTS         the compaction timestamp
    * @param cellsPerPartition approximate maximum number of cells (numRows * numColumns) to be in each partition file.
    *                          Adjust this to control output file size,
    * @return
    */
  protected def commitHotToCold(compactTS: Timestamp, cellsPerPartition: Int): Try[AuditTableFile] = {
    val hotRegions = regions.filter(r => !r.is_deprecated && r.store_type == HOT_PARTITION)
    compactRegions(hotPath, hotRegions, compactTS, cellsPerPartition)
  }

  /**
    * Merges all regions with row numbers below a specific threshold into one cold region.
    *
    * @param compactTS               the compaction timestamp
    * @param smallRegionRowThreshold the row number threshold to use for determinining small regions to be compacted
    * @param cellsPerPartition       approximate maximum number of cells (numRows * numColumns) to be in each partition file.
    *                                Adjust this to control output file size,
    * @return
    */
  protected def compactCold(compactTS: Timestamp, smallRegionRowThreshold: Int, cellsPerPartition: Int): Try[AuditTableFile] = {
    val smallerRegions = regions.filter(r => r.store_type == COLD_PARTITION && r.count < smallRegionRowThreshold)
    // No use compacting a single small region into itself
    compactRegions(coldPath, if (smallerRegions.length < 2) Seq.empty else smallerRegions, compactTS, cellsPerPartition)
  }

  protected def compactRegions(typePath: Path, toCompact: Seq[AuditTableRegionInfo], compactTS: Timestamp, cellsPerPartition: Int): Try[AuditTableFile] = {
    Try {
      val res = if (toCompact.isEmpty) new AuditTableFile(this.tableInfo, this.regions, this.storageOps, this.baseFolder, this.newRegionID)
      else {
        val ids = toCompact.map(_.store_region)
        val regionID = newRegionID(this)
        val regionPath = new Path(coldPath, s"$STORE_REGION_COLUMN=" + regionID)
        logInfo(s"Compacting regions ${ids.mkString("[", ", ", "]")} in path [${regionPath.toString}]")
        if (storageOps.pathExists(regionPath)) throw StorageException(s"Can not compact table [${tableName}], as path [${regionPath.toString}] already exists")

        val data = storageOps.openParquet(typePath)
        val newRegionSet = data.map { rows =>
          val currentNumPartitions = rows.rdd.getNumPartitions
          val newNumPartitions = calculateNumPartitions(rows.schema, toCompact.map(_.count).sum, cellsPerPartition)
          val hotRows = rows.filter(rows(STORE_REGION_COLUMN).isin(ids: _*)).drop(STORE_REGION_COLUMN)
          val hotRowsRepartitioned = if (newNumPartitions > currentNumPartitions) {
            hotRows.repartition(newNumPartitions)
          } else hotRows.coalesce(newNumPartitions)
          storageOps.atomicWriteAndCleanup(tableInfo.table_name, hotRowsRepartitioned, regionPath, typePath, ids.map(r => s"$STORE_REGION_COLUMN=$r"), compactTS)
          val (count, max_latest_ts) = calcRegionStats(storageOps.openParquet(regionPath).get)
          val idSet = ids.toSet
          val remainingRegions = regions.filter(r => !idSet.contains(r.store_region))
          val region = AuditTableRegionInfo(tableInfo.table_name, COLD_PARTITION, regionID, compactTS, false, count, max_latest_ts)
          logInfo(s"Compacted region ${region.toString} was created.")
          remainingRegions :+ region
        }
        newRegionSet.fold(this)(r => setRegions(this, r))
      }
      res
    }
  }

  protected def calculateNumPartitions(schema: StructType, numRows: Long, cellsPerPartition: Long): Int = {
    val cellsPerRow = schema.size
    val rowsPerPartition = cellsPerPartition / cellsPerRow
    println(s"CELLS PER ROW: $cellsPerRow, ROWS PER PARTITION: $rowsPerPartition, NUM ROWS: $numRows, NUM PARTITIONA: ${(numRows / rowsPerPartition).toInt + 1}")
    (numRows / rowsPerPartition).toInt + 1
  }

  protected def calcRegionStats(ds: Dataset[_]): (Long, Timestamp) = {
    ds.select(count(ds(DE_LAST_UPDATED_COLUMN)), max(ds(DE_LAST_UPDATED_COLUMN)))
      .collect().map(r => (r.getAs[Long](0), Option(r.getAs[Timestamp](1)).getOrElse(lowTimestamp)))
      .head
  }

  /**
    * Each function that modifies the state of the storage layer must call this function in the first line. As
    * audit's table state can be modified only once.
    */
  protected def markToUpdate(): Unit = if (wasModified) throw StorageException(s"Table [$tableName] can no longer be updated.") else wasModified = true

}

object AuditTableFile extends Logging {

  val STORE_TYPE_COLUMN = "de_store_type"

  val STORE_REGION_COLUMN = "de_store_region"

  val DE_LAST_UPDATED_COLUMN = "_de_last_updated"

  val HOT_PARTITION = "hot"

  val COLD_PARTITION = "cold"

  val REGION_INFO_DIRECTORY = ".regioninfo"

  val lowTimestamp: Timestamp = Timestamp.valueOf("0001-01-01 00:00:00")

  val highTimestamp: Timestamp = Timestamp.valueOf("9999-12-31 23:59:59")

  /**
    * Generic function that generates sequential region ids that are padded on the left with zeros up to 20 chars.
    *
    * @param table
    * @return
    */
  def nextLongRegion(table: AuditTableFile): String = f"r${table.activeRegionIDs().map(_.max.drop(1).toLong + 1).getOrElse(0L)}%020d"

  /**
    * Creates a copy of the table with new list of regions.
    *
    * @param audit
    * @param regions
    * @return
    */
  def setRegions(audit: AuditTableFile, regions: Seq[AuditTableRegionInfo]): AuditTableFile = {
    val spark = audit.storageOps.sparkSession
    import spark.implicits._
    val regionInfoDS = audit.storageOps.sparkSession.createDataset(regions).coalesce(1)
    audit.storageOps.writeParquet(audit.tableName, new Path(audit.regionInfoBasePath, audit.tableName), regionInfoDS)
    new AuditTableFile(audit.tableInfo, regions, audit.storageOps, audit.baseFolder, audit.newRegionID)
  }

  /**
    * In one spark job scans all of the specified tables and infers stats about each region of the listed tables.
    *
    * @param sparkSession
    * @param fileStorage
    * @param basePath
    * @param tableNames
    * @param includeHot if true, than hot regions will be included in the scan. By default is true. False is useful
    *                   when reading production data from dev environments, as compactions will be happening in out
    *                   of office hours, this helps to avoid reading data in an inconsistent state.
    * @return
    */
  def inferRegionsWithStats(sparkSession: SparkSession, fileStorage: FileStorageOps, basePath: Path, tableNames: Seq[String], includeHot: Boolean = true, skipRegionInfoCache: Boolean = false): Seq[AuditTableRegionInfo] = {

    val fromCache = if (skipRegionInfoCache) Seq.empty else inferRegionsFromCache(fileStorage, basePath, tableNames, includeHot)

    //Only infer regions using paths and parquet from tables that are not in the cache
    val tablesMissingFromCache = (tableNames.toSet diff fromCache.map(_.table_name).toSet).toSeq
    val fromPaths = inferRegionsFromPaths(fileStorage, basePath, tablesMissingFromCache, includeHot).map(t => (t.table_name, t.store_type, t.store_region) -> t).toMap
    val fromParquet = inferRegionsFromParquet(sparkSession, fileStorage, basePath, tablesMissingFromCache, includeHot).map(t => (t.table_name, t.store_type, t.store_region) -> t).toMap

    //Merge regions, take preference for fromParquet, then combine with cache-backed region info
    (fromPaths.keySet ++ fromParquet.keySet).toSeq.map(k => fromParquet.getOrElse(k, fromPaths(k))) ++ fromCache
  }

  private[storage] def inferRegionsFromCache(fileStorage: FileStorageOps, basePath: Path, tableNames: Seq[String], includeHot: Boolean): Seq[AuditTableRegionInfo] = {

    val parFun: PartialFunction[FileStatus, Path] = {
      case r if r.isDirectory => r.getPath
    }

    val presentTables = fileStorage.globTablePaths(new Path(basePath, REGION_INFO_DIRECTORY), tableNames, Seq.empty, parFun).toList
    presentTables match {
      case h :: tail =>
        // Read cache, filter if needed and collect
        val spark = fileStorage.sparkSession
        import spark.implicits._
        val cache = fileStorage.openParquet(h, tail: _*)
          .map(
            _.as[AuditTableRegionInfo]
              .filter(includeHot || _.store_type != HOT_PARTITION)
              .collect()
              .toSeq
          )
        cache.getOrElse {
          logWarning(s"Unable to read region cache info for tables: [${presentTables.map(_.getName).mkString(", ")}]. " +
            s"Defaulting to reading from file and/or parquet; this will affect performance.")
          Seq.empty[AuditTableRegionInfo]
        }
      case Nil => Seq.empty
    }

  }

  /**
    * Infer all regions using information found in Parquet files only. This will not include regions that have no data.
    * Use [[inferRegionsFromPaths]] to find information about regions with empty Parquet files.
    */
  private def inferRegionsFromParquet(sparkSession: SparkSession, fileStorage: FileStorageOps, basePath: Path, tableNames: Seq[String], includeHot: Boolean)
  : Seq[AuditTableRegionInfo] = {
    val spark = sparkSession
    import spark.implicits._

    val regions = tableNames.iterator.grouped(20).flatMap { page =>
      val pageDFs: Seq[DataFrame] = page.map { table =>
        val data = fileStorage.openParquet(new Path(basePath, table))
        data.map(df => if (includeHot) df.filter(df(STORE_TYPE_COLUMN).isin(HOT_PARTITION, COLD_PARTITION)) else df.filter(df(STORE_TYPE_COLUMN).isin(COLD_PARTITION)))
          .map { df =>
            df.groupBy(df(STORE_TYPE_COLUMN), df(STORE_REGION_COLUMN))
              .agg(count(df(DE_LAST_UPDATED_COLUMN)).as("count"), max(df(DE_LAST_UPDATED_COLUMN)).as("max_last_updated"))
              .select(
                lit(table).as("table_name")
                , df(STORE_TYPE_COLUMN).as("store_type")
                , df(STORE_REGION_COLUMN).as("store_region")
                // TODO: can we actually carry this in the data?
                , lit(lowTimestamp).as("created_on")
                , lit(false).as("is_deprecated")
                , $"count"
                , $"max_last_updated"
              )
          }
      }.filter(_.isDefined).map(_.get)
      if (pageDFs.isEmpty) Seq.empty
      else pageDFs.reduce(_ union _).as[AuditTableRegionInfo].collect()
    }.toSeq
    regions
  }

  /**
    * Infer all regions using information found in paths only. This will not include any specific information about region details.
    * Counts will be 0, and all timestamps will be [[lowTimestamp]]. This information should be augmented with details from [[inferRegionsFromParquet]].
    */
  private def inferRegionsFromPaths(fileStorage: FileStorageOps, basePath: Path, tableNames: Seq[String], includeHot: Boolean): Seq[AuditTableRegionInfo] = {

    val parFun: PartialFunction[FileStatus, AuditTableRegionInfo] = {
      case r if r.isDirectory => val path = r.getPath
        AuditTableRegionInfo(
          path.getParent.getParent.getName,
          path.getParent.getName.split('=')(1),
          path.getName.split('=')(1),
          lowTimestamp,
          false,
          0,
          lowTimestamp
        )
    }

    fileStorage.globTablePaths(basePath, tableNames, Seq(s"$STORE_TYPE_COLUMN=${if (includeHot) "*" else COLD_PARTITION}", s"$STORE_REGION_COLUMN=*"), parFun)

  }

  /**
    * Reads the state of the multiple Audit Tables. It will scan the state of regions of all specified tables in one go.
    *
    * @param sparkSession
    * @param fileStorage object that actually interacts with the physical storage
    * @param basePath    parent folder which contains folders with table names
    * @param newRegionID function that generates region ids
    * @param tableNames  list of tables to open
    * @param includeHot  include hot regions in the table
    * @return (Map[TABLE NAME, AuditTable], Seq[MISSING TABLES]) - audit table objects that exist and
    *         of table names that were not found under the basePath
    */
  def openTables(sparkSession: SparkSession, fileStorage: FileStorageOps, basePath: Path
                 , tableNames: Seq[String], includeHot: Boolean = true)(newRegionID: (AuditTableFile) => String): (Map[String, Try[AuditTableFile]], Seq[String]) = {
    val existingTables = fileStorage.listTables(basePath).toSet
    val (exist, not) = tableNames.partition(existingTables.contains)
    val tableRegions = inferRegionsWithStats(sparkSession, fileStorage, basePath, exist, includeHot).groupBy(_.table_name)
    // some might not have regions yet
    val tablesObjs = exist.foldLeft(Map.empty[String, Try[AuditTableFile]]) { (res, tableName) =>
      val info = fileStorage.readAuditTableInfo(basePath, tableName)
      val regions = tableRegions.getOrElse(tableName, Seq.empty[AuditTableRegionInfo])
      res + (tableName -> info.map(i => new AuditTableFile(i, regions, fileStorage, basePath, newRegionID)))
    }
    (tablesObjs, not)
  }

  /**
    * Creates a table in the physical storage layer.
    *
    * @param sparkSession
    * @param fileStorage
    * @param basePath
    * @param tableInfo table metadata
    * @param newRegionID
    * @return
    */
  def createTable(sparkSession: SparkSession, fileStorage: FileStorageOps, basePath: Path
                  , tableInfo: AuditTableInfo)(newRegionID: (AuditTableFile) => String): Try[AuditTableFile] = {
    val table = new AuditTableFile(tableInfo, Seq.empty, fileStorage, basePath, newRegionID)
    table.initNewTable()
  }
}

/**
  * Static information about the table, that is persisted when audit table is initialised.
  *
  * @param table_name   name of the table
  * @param primary_keys list of columns that make up primary key, these will be used for snapshot generation and
  *                     record deduplication
  * @param meta         application/custom metadata that will not be used in this library.
  */
case class AuditTableInfo(table_name: String, primary_keys: Seq[String], meta: Map[String, String])

/**
  *
  * @param table_name       name of the table
  * @param store_type       cold or hot, appended regions are added to hot and after compaction make it into cold. Cold
  *                         regions can also be compacted
  * @param store_region     id of the region, for simplicity, at least for now it will be GUID
  * @param created_on       timestamp when region was created as a result of an append or compact operation
  * @param is_deprecated    true - its data was compacted into another region, false - it was not compacted
  * @param count            number of records in the region, can be used for optimisation and compaction decisions
  * @param max_last_updated all records in the audit table will contain column that shows the last updated time,
  *                         this will be used to generated ingestion queries
  */
case class AuditTableRegionInfo(table_name: String, store_type: String, store_region: String, created_on: Timestamp
                                , is_deprecated: Boolean, count: Long, max_last_updated: Timestamp)
