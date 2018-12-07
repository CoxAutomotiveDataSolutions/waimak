package com.coxautodata.waimak.storage

import java.io.{InputStreamReader, OutputStreamWriter}
import java.sql.Timestamp
import java.time.Duration
import java.util.Properties

import com.coxautodata.waimak.filesystem.FSUtils
import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

/**
  * Contains operations that interact with physical storage. Will also handle commit to the file system.
  *
  * Created by Alexei Perelighin on 2018/03/05
  */
trait FileStorageOps {

  def sparkSession: SparkSession

  /**
    * Opens parquet file from the path, which can be folder or a file.
    * If there are partitioned sub-folders with file with slightly different schema, it will attempt to merge schema
    * to accommodate for the schema evolution.
    *
    * @param path path to open
    * @return Some with dataset if there is data, None if path does not exist or can not be opened
    * @throws Exception in cases of connectivity
    */
  def openParquet(path: Path, paths: Path*): Option[Dataset[_]]

  /**
    * Commits data set into full path. The path is the full path into which the parquet will be placed after it is
    * fully written into the temp folder.
    *
    * @param tableName     name of the table, will only be used to write into tmp
    * @param path          full destination path
    * @param ds            dataset to write out. no partitioning will be performed on it
    * @param overwrite     whether to overwrite the existing data in `path`. If false folder contents will be merged
    * @param tempSubfolder an optional subfolder used for writing temporary data, used like `$temp/$tableName/$tempSubFolder`.
    *                      If not given, then path becomes: `$temp/$tableName/${path.getName}`
    * @throws           Exception can be thrown due to access permissions, connectivity, spark UDFs (as datasets are lazily executed)
    */
  def writeParquet(tableName: String, path: Path, ds: Dataset[_], overwrite: Boolean = true, tempSubfolder: Option[String] = None): Unit

  /**
    * Checks if the path exists in the physical storage.
    *
    * @param path
    * @return true if path exists in the storage layer
    */
  def pathExists(path: Path): Boolean

  /**
    * Creates folders on the physical storage.
    *
    * @param path path to create
    * @return true if the folder exists or was created without problems, false if there were problems creating all folders in the path
    */
  def mkdirs(path: Path): Boolean

  /**
    * During compaction, data from multiple folders need to be merged and re-written into one folder with fewer files.
    * The operation has to be fail safe; moving out data can only take place after new version is fully written and committed.
    *
    * E.g. data from fromBase=/data/db/tbl1/type=hot and fromSubFolders=Seq("region=11", "region=12", "region=13", "region=14")
    * will be merged and coalesced into optimal number of partitions in Dataset data and will be written out into
    * newDataPath=/data/db/tbl1/type=cold/region=15 with old folder being moved into table's trash folder.
    *
    * Starting state:
    *
    * /data/db/tbl1/type=hot/region=11
    * .../region=12
    * .../region=13
    * .../region=14
    *
    * Final state:
    *
    * /data/db/tbl1/type=cold/region=15
    * /data/db/.Trash/tbl1/${appendTimestamp}/region=11
    * .../region=12
    * .../region=13
    * .../region=14
    *
    * @param tableName       name of the table
    * @param compactedData   the data set with data from fromSubFolders already repartitioned, it will be saved into
    *                        newDataPath
    * @param newDataPath     path into which combined and repartitioned data from the dataset will be committed into
    * @param cleanUpBase     parent folder from which to remove the cleanUpFolders
    * @param cleanUpFolders  list of sub-folders to remove once the writing and committing of the combined data is successful
    * @param appendTimestamp Timestamp of the compaction/append. Used to date the Trash folders.
    */
  def atomicWriteAndCleanup(tableName: String, compactedData: Dataset[_], newDataPath: Path, cleanUpBase: Path, cleanUpFolders: Seq[String], appendTimestamp: Timestamp)

  /**
    * Purge the trash folder for a given table. All trashed region folders that were placed into the trash
    * older than the given maximum age will be deleted.
    *
    * @param tableName       Name of the table to purge the trash for
    * @param appendTimestamp Timestamp of the current compaction/append. All ages will be compared
    *                        relative to this timestamp
    * @param trashMaxAge     Maximum age of trashed regions to keep relative to the above timestamp
    */
  def purgeTrash(tableName: String, appendTimestamp: Timestamp, trashMaxAge: Duration): Unit

  /**
    * Lists tables in the basePath. It will ignore any folder/table that starts with '.'
    *
    * @param basePath parent folder which contains folders with table names
    * @return
    */
  def listTables(basePath: Path): Seq[String]

  /**
    * Writes out static data about the audit table into basePath/table_name/.table_info file.
    *
    * @param basePath parent folder which contains folders with table names
    * @param info     static information about table, that will not change during table's existence
    */
  def writeAuditTableInfo(basePath: Path, info: AuditTableInfo): Try[AuditTableInfo]

  /**
    * Reads the table info back.
    *
    * @param basePath  parent folder which contains folders with table names
    * @param tableName name of the table to read for
    * @return
    */
  def readAuditTableInfo(basePath: Path, tableName: String): Try[AuditTableInfo]

  /**
    * Glob a list of table paths with partitions, and apply a partial function to collect (filter+map) the result to transform
    * the FileStatus to any type `A`
    *
    * @param basePath        parent folder which contains folders with table names
    * @param tableNames      list of table names to search under
    * @param tablePartitions list of partition columns to include in the path
    * @param parFun          a partition function to transform FileStatus to any type `A`
    * @tparam A return type of final sequence
    * @return
    */
  def globTablePaths[A](basePath: Path, tableNames: Seq[String], tablePartitions: Seq[String], parFun: PartialFunction[FileStatus, A]): Seq[A]

}

/**
  * Implementation around FileSystem and SparkSession with temporary and trash folders.
  *
  * @param fs
  * @param sparkSession
  * @param tmpFolder
  * @param trashBinFolder
  */
class FileStorageOpsWithStaging(fs: FileSystem, override val sparkSession: SparkSession, tmpFolder: Path, trashBinFolder: Path)
  extends FileStorageOps with Logging {

  override def openParquet(path: Path, paths: Path*): Option[Dataset[_]] = {
    val allPaths = path +: paths
    val allPathsString = allPaths.map(_.toString)
    if (allPaths.forall(fs.exists)) {
      Try(sparkSession.read.option("mergeSchema", "true").parquet(allPathsString: _*)) match {
        case Success(df) => Some(df)
        case Failure(e: AnalysisException) if e.getMessage().contains("Unable to infer schema") =>
          logWarning(s"Cannot open parquet(s) at [${allPathsString.mkString(", ")}]", e)
          None
        case Failure(e) => {
          logError(s"Can not open parquet in path: [${allPathsString.mkString(", ")}]", e)
          throw e
        }
      }
    } else None
  }

  override def writeParquet(tableName: String, path: Path, ds: Dataset[_], overwrite: Boolean = true, tempSubfolder: Option[String] = None): Unit = {
    val writePath = new Path(new Path(tmpFolder, tableName), tempSubfolder.getOrElse(path.getName))
    ds.write.mode(SaveMode.Overwrite).parquet(writePath.toString)
    if (overwrite) {
      if (!FSUtils.moveOverwriteFolder(fs, writePath, path)) throw StorageException(s"Can not overwrite data for table [$tableName] into folder [${path.toString}]")
    } else {
      Try(FSUtils.mergeMoveFiles(fs, writePath, path, f => f.getName.toLowerCase.startsWith("part-")))
        .recover { case e => throw StorageException(s"Failed to merge table files for table [$tableName] from [$writePath] to [$path]", e) }
        .get
      if (!fs.delete(writePath, true)) throw StorageException(s"Could not remove temp folder [$writePath] for table [$tableName]")
    }
  }

  override def pathExists(path: Path): Boolean = fs.exists(path)

  override def mkdirs(path: Path): Boolean = fs.mkdirs(path)

  override def atomicWriteAndCleanup(tableName: String, data: Dataset[_], newDataPath: Path, cleanUpBase: Path, cleanUpFolders: Seq[String], appendTimestamp: Timestamp): Unit = {
    val writePath = new Path(new Path(tmpFolder, tableName), newDataPath.getName)
    data.write.mode(SaveMode.Overwrite).parquet(writePath.toString)
    if (!FSUtils.moveOverwriteFolder(fs, writePath, newDataPath)) throw new StorageException(s"Can not write compacted data for table [${tableName}] into folder [${newDataPath.toString}]")
    val trashRootPath = new Path(trashBinFolder, tableName)
    val trashPath = new Path(trashRootPath, appendTimestamp.getTime.toString)
    cleanUpFolders.foreach { fName =>
      val from = new Path(cleanUpBase, fName)
      val trash = new Path(trashPath, fName)
      if (!FSUtils.moveOverwriteFolder(fs, from, trash)) throw new StorageException(s"Can not move old folder [${from.toString}] into folder [${trash.toString}]")
    }
  }

  override def purgeTrash(tableName: String, appendTimestamp: Timestamp, trashMaxAge: Duration): Unit = {
    val minTimestampToKeep = appendTimestamp.getTime - trashMaxAge.toMillis

    val probTSAndOlderThanMin: PartialFunction[FileStatus, Path] = {
      case f if f.getPath.getName.matches("[0-9]+") && f.getPath.getName.toLong < minTimestampToKeep => f.getPath
    }

    val foldersToDelete = globTablePaths(trashBinFolder, Seq(tableName), Seq("*"), probTSAndOlderThanMin)
    foldersToDelete.foreach {
      f => FSUtils.removeFolder(fs, f.toString)
    }
  }

  override def listTables(basePath: Path): Seq[String] = {
    if (fs.exists(basePath)) {
      fs.listStatus(basePath).map(_.getPath.getName).filter(!_.startsWith("."))
    } else Seq.empty
  }

  override def writeAuditTableInfo(basePath: Path, auditTableInfo: AuditTableInfo): Try[AuditTableInfo] = {
    Try {
      val outputPath = new Path(new Path(basePath, auditTableInfo.table_name), ".table_info")
      val writer = fs.create(outputPath, true)
      val wr = new OutputStreamWriter(writer, "UTF-8")
      wr.write(s"table_name=${auditTableInfo.table_name}\n")
      wr.write(s"primary_keys=${auditTableInfo.primary_keys.mkString("|")}\n")
      auditTableInfo.meta.foreach(kv => wr.write(s"meta.${kv._1}=${kv._2}\n"))
      wr.close()
      auditTableInfo
    }
  }

  override def readAuditTableInfo(basePath: Path, tableName: String): Try[AuditTableInfo] = {
    Try {
      import scala.collection.JavaConverters._

      val input = new Path(new Path(basePath, tableName), ".table_info")
      val config: Properties = new Properties()
      val stream = fs.open(input)
      val stream2 = new InputStreamReader(stream, "UTF-8")
      config.load(stream2)
      stream.close()

      val meta = config.keySet().asScala.map(_.toString).filter(_.startsWith("meta."))
        .foldLeft(Map.empty[String, String])((res, key) => {
          val v = config.getProperty(key)
          res + (key.drop(5) -> v)
        })
      val primary = config.getProperty("primary_keys").split("\\|")
      AuditTableInfo(tableName, primary, meta)
    }
  }

  override def globTablePaths[A](basePath: Path, tableNames: Seq[String], tablePartitions: Seq[String], parFun: PartialFunction[FileStatus, A]): Seq[A] = {
    val globPath = tablePartitions.foldLeft(new Path(basePath, tableNames.mkString("{", ",", "}")))((p, c) => new Path(p, c))
    val results = fs.globStatus(globPath)
    results.collect(parFun)
  }

}