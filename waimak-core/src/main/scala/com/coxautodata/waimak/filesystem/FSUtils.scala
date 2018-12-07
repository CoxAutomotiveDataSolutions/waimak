package com.coxautodata.waimak.filesystem

import com.coxautodata.waimak.log.Logging
import org.apache.hadoop.fs._

/**
  *
  * Created by Alexei Perelighin on 23/10/17.
  */
object FSUtils extends Logging {

  /**
    * Implicit class to convert an Hadoop RemoteIterator object to a Scala Iterator
    *
    * @param underlying Underlying RemoteIterator object
    * @tparam T Type of the elements in the iterator
    */
  implicit class ScalaRemoteIterator[T](underlying: RemoteIterator[T]) extends Iterator[T] {
    override def hasNext: Boolean = underlying.hasNext

    override def next(): T = underlying.next()
  }

  /**
    * Check is objects in the toTest collection can be mapped to existing folders in the HDFS and returns
    * objects that have not been mapped into the HDFS folder yet.
    *
    * Implementation is quite efficient as it uses HDFS PathFilter and does not use globs or full lists that could be
    * quite big.
    *
    * For example:
    * Inputs:
    * 1) HDFS folder inParentFolder contains partition folders, one per day from 2017/01/01 to 2017/03/15
    * 2) toTest is a suggested range of dates from 2017/03/10 to 2017/03/20
    *
    * Output:
    * 1) list of dates from 2017/03/16 to 2017/03/20
    *
    * @param fs
    * @param inParentFolder - HDFS folder that contains folders that could be mapped to tested objects
    * @param toTest
    * @param getObjectPath  - maps hdfs path to tested object
    * @tparam O
    * @return objects from toTest that could not be mapped into the folder inParentFolder via function getObjectPath
    */
  def keepNotPresent[O](fs: FileSystem, inParentFolder: Path, toTest: Seq[O])(getObjectPath: O => Path): Seq[O] = {
    //converted existing paths to Map[TableName, Set[partitionName]]
    val plannedOutput = toTest.map(getObjectPath).map(_.toString).toSet
    // In case of a lot of partitions in all of the tables, filter pushed into HDFS layer will reduce size of the returned array helping with memory usage
    val sameAsForObjects = new PathFilter {

      override def accept(path: Path): Boolean = {
        val rem = path.toUri.getPath
        val res = plannedOutput.contains(rem)
        res
      }

    }
    //convert existing paths to Map[TableName, Set[partitionName]]
    val alreadyExisting = {
      if (fs.exists(inParentFolder)) {
        fs.globStatus(new Path(inParentFolder.toString + "/*/*"), sameAsForObjects).map(_.getPath.toUri.getPath.toString).toSet
      } else {
        Set.empty[String]
      }
    }
    logInfo("Already existing: " + alreadyExisting)
    // remove all objects that could collide with existing tables and partitions
    toTest.filter { o =>
      val testPath = getObjectPath(o).toString
      !alreadyExisting.contains(testPath)
    }
  }

  /**
    * Deletes folder with all of its content, if it does not exist than does nothing.
    *
    * @param fs
    * @param folder
    */
  def removeFolder(fs: FileSystem, folder: String): Unit = {
    val path = new Path(folder)
    if (fs.exists(path)) fs.delete(path, true)
  }

  /**
    * Lists Hive partition column name and its value, by looking into the folder.
    *
    * @param fs
    * @param folder
    * @return (PARTITON COLUMN NAME, VALUE)
    */
  def listPartitions(fs: FileSystem, folder: String): Seq[(String, String)] = {
    val path = new Path(folder)
    if (!fs.exists(path)) Seq.empty
    else {
      fs.listStatus(path).map(_.getPath.getName).filter(_.contains("=")).map(_.split("=")).map(p => (p(0), p(1)))
    }
  }

  /**
    * Moves toMove into toPath. Parent folder of the toPath is created if it does not exist
    *
    * @param fs     - FileSystem which can be HDFS or Local.
    * @param toMove - full path to the folder to be moved.
    * @param toPath - full path to be moved into, includes the folder name itself.
    * @return true if move was successful.
    */
  def moveOverwriteFolder(fs: FileSystem, toMove: Path, toPath: Path): Boolean = {
    if (!fs.exists(toPath.getParent)) {
      logInfo("Create parent folder " + toPath.getParent)
      fs.mkdirs(toPath.getParent)
    }
    if (fs.exists(toMove)) {
      logInfo(s"Removing ${toPath.toString} in order to replace.")
      fs.delete(toPath, true)
    }
    val committed = fs.rename(toMove, toPath)
    logInfo(s"${committed} move of [${toMove.toString}] into [${toPath}]")
    committed
  }

  def mergeMoveFiles(fs: FileSystem, sourceFolder: Path, destinationFolder: Path, pathFilter: Path => Boolean): Unit = {
    if (!fs.exists(sourceFolder)) throw new PathNotFoundException(s"Source folder [$sourceFolder]")
    if (!fs.getFileStatus(sourceFolder).isDirectory) throw new PathIsNotDirectoryException(s"Source path is not a directory [$sourceFolder]")
    if (!fs.exists(destinationFolder)) {
      logInfo(s"Creating folder $destinationFolder")
      if (!fs.mkdirs(destinationFolder)) throw new PathOperationException(s"Could not create folder [$destinationFolder]")
    }
    fs.listFiles(sourceFolder, false)
      .filter(f => f.isFile && pathFilter(f.getPath))
      .foreach{
        f =>
          val destPath = new Path(destinationFolder, f.getPath.getName)
          if (fs.exists(destPath)) throw new PathExistsException(s"Cannot move [${f.getPath}] to [$destPath] as a file already exists")
          if (!fs.rename(f.getPath, destPath)) throw new PathOperationException(s"Failed to rename [${f.getPath}] to [$destPath]")
          logDebug(s"Moved file [${f.getPath}] to [$destPath]")
      }
  }

  /**
    * Check if there are any existing folders with the same name in the path and removes them. The main benefit is that
    * it performs checks in one round-trip to HDFS which in case of day zero scenarios could take a lot of time.
    *
    * @param fs
    * @param folder - parent folder in which to check for existing sub-folders
    * @param subs   - names to check, if the name is not present, than ignore it, if present, remove it
    * @return - true if everything was fine
    */
  def removeSubFoldersPresentInList(fs: FileSystem, folder: Path, subs: Seq[String]): Boolean = {
    val set = subs.toSet
    val pathFilter = new PathFilter {

      override def accept(path: Path): Boolean = set.contains(path.getName)

    }

    val toRemove = fs.listStatus(folder, pathFilter).map(_.getPath)
    toRemove.foreach(p => logInfo(s"Removing ${p.toString}."))
    val removed = toRemove.map(p => fs.delete(p, true)).fold(true)((r, e) => r && e) //fold works on empty lists in case nothing was removed
    removed
  }

  /**
    * Moves all sub-folders in fromPath into toPath. If a folder exists in the destination, it is overwritten.
    * It uses and efficient approach to minimise the number of call to HDFS for checks and validations which could add
    * significant amount of time to the end to end execution.
    *
    * @param fs       - current hadoop file system
    * @param subs     - sub folders to move, usually thsese are folders in the staging folder
    * @param fromPath - parent folder in which sub folders are
    * @param toPath   - into which folder to move the subs folders, if any already exist, then need to be overwritten
    * @return
    */
  def moveAll(fs: FileSystem, subs: Seq[String], fromPath: Path, toPath: Path): Boolean = {
    if (!fs.exists(toPath)) {
      logInfo("Create folder " + toPath)
      fs.mkdirs(toPath)
    }
    if (removeSubFoldersPresentInList(fs, toPath, subs)) {
      subs.map { f =>
        val from = new Path(fromPath, f)
        val to = new Path(toPath, f)
        val res = fs.rename(from, to)
        logInfo(s"${res} move of [${from.toString}] into [${to}]")
        res
      }.fold(true)((r, e) => r && e)
    } else false
  }
}

class HSRemoteIterator[A](it: RemoteIterator[A]) extends Iterator[A] {

  override def hasNext: Boolean = it.hasNext

  override def next(): A = it.next()

}