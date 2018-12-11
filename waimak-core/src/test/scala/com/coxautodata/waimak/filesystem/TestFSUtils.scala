package com.coxautodata.waimak.filesystem

import com.coxautodata.waimak.dataflow.spark.SparkAndTmpDirSpec
import org.apache.hadoop.fs._

class TestFSUtils extends SparkAndTmpDirSpec {
  override val appName: String = "FSUtils"

  var fs: FileSystem = _
  var srcDir: Path = _
  var destDir: Path = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    srcDir = new Path(tmpDir, "src")
    destDir = new Path(tmpDir, "dest")
  }

  describe("mergeMoveFiles") {

    it("should create the destination directory and move files") {
      val filename = "test"
      fs.mkdirs(srcDir)
      val srcFile = new Path(srcDir, filename)
      val destFile = new Path(destDir, filename)
      fs.create(srcFile).close()
      fs.exists(srcFile) should be(true)
      fs.exists(destDir) should be(false)
      FSUtils.mergeMoveFiles(fs, srcDir, destDir, _ => true)
      fs.exists(srcFile) should be(false)
      fs.exists(srcDir) should be(true)
      fs.exists(destDir) should be(true)
      fs.exists(destFile) should be(true)
    }

    it("should move files if the destination directory exists") {
      val filename = "test"
      fs.mkdirs(srcDir)
      fs.mkdirs(destDir)
      val srcFile = new Path(srcDir, filename)
      val destFile = new Path(destDir, filename)
      fs.create(srcFile).close()
      fs.exists(srcFile) should be(true)
      fs.exists(destDir) should be(true)
      FSUtils.mergeMoveFiles(fs, srcDir, destDir, _ => true)
      fs.exists(srcFile) should be(false)
      fs.exists(srcDir) should be(true)
      fs.exists(destDir) should be(true)
      fs.exists(destFile) should be(true)
    }

    it("should create an empty directory if no files are to be copied") {
      fs.mkdirs(srcDir)
      fs.exists(destDir) should be(false)
      FSUtils.mergeMoveFiles(fs, srcDir, destDir, _ => true)
      fs.exists(srcDir) should be(true)
      fs.exists(destDir) should be(true)
    }

    it("should move only files that match the filter") {
      val filenameGood = "good"
      val filenameBad = "bad"
      fs.mkdirs(srcDir)
      val srcFileGood = new Path(srcDir, filenameGood)
      val destFileGood = new Path(destDir, filenameGood)
      val srcFileBad = new Path(srcDir, filenameBad)
      val destFileBad = new Path(destDir, filenameBad)
      fs.create(srcFileGood).close()
      fs.exists(srcFileGood) should be(true)
      fs.exists(destFileGood) should be(false)
      fs.create(srcFileBad).close()
      fs.exists(srcFileBad) should be(true)
      fs.exists(destFileBad) should be(false)
      FSUtils.mergeMoveFiles(fs, srcDir, destDir, p => p.getName == "good")
      fs.exists(srcFileGood) should be(false)
      fs.exists(destFileGood) should be(true)
      fs.exists(srcFileBad) should be(true)
      fs.exists(destFileBad) should be(false)
    }

    it("should move only files and not directories") {
      val filenameGood = "good"
      val filenameBad = "bad"
      fs.mkdirs(srcDir)
      val srcFileGood = new Path(srcDir, filenameGood)
      val destFileGood = new Path(destDir, filenameGood)
      val srcFileBad = new Path(srcDir, filenameBad)
      val destFileBad = new Path(destDir, filenameBad)
      fs.create(srcFileGood).close()
      fs.exists(srcFileGood) should be(true)
      fs.exists(destFileGood) should be(false)
      fs.mkdirs(srcFileBad)
      fs.exists(srcFileBad) should be(true)
      fs.exists(destFileBad) should be(false)
      FSUtils.mergeMoveFiles(fs, srcDir, destDir, _ => true)
      fs.exists(srcFileGood) should be(false)
      fs.exists(destFileGood) should be(true)
      fs.exists(srcFileBad) should be(true)
      fs.exists(destFileBad) should be(false)
    }

    it("should throw an exception if the file already exists in the destination") {
      val filename = "test"
      fs.mkdirs(srcDir)
      fs.mkdirs(destDir)
      val srcFile = new Path(srcDir, filename)
      val destFile = new Path(destDir, filename)
      fs.create(srcFile).close()
      fs.create(destFile).close()
      val e = intercept[PathExistsException] {
        FSUtils.mergeMoveFiles(fs, srcDir, destDir, _ => true)
      }
      e.getMessage should be(s"`Cannot move [file:$srcFile] to [$destFile] as a file already exists': File exists")
    }

    it("should throw an exception if the source folder does not exist") {
      val e = intercept[PathNotFoundException] {
        FSUtils.mergeMoveFiles(fs, srcDir, destDir, _ => true)
      }
      e.getMessage should be(s"`Source folder [$srcDir] not found': No such file or directory")
    }

    it("should throw an exception if the source path is not a folder") {
      fs.create(srcDir).close()
      val e = intercept[PathIsNotDirectoryException] {
        FSUtils.mergeMoveFiles(fs, srcDir, destDir, _ => true)
      }
      e.getMessage should be(s"`Source path is not a directory [$srcDir]': Is not a directory")
    }

  }


}
