package ot.scalaotl
package config

import java.io.{File, IOException}
import java.nio.file.Paths
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.DataFrame

import scala.util.Try

trait OTLLookups extends OTLConfig {
  // Section with backward compability for Search and Example classes. Will be removed soon.
  val lookupFolder = Paths.get(otlconfig.getString("lookups.path")).toAbsolutePath
  def isExist(file: String) = new File(getFullLookupPath(file)).exists
  def getFullLookupPath(file: String) = lookupFolder.resolve(file).toString
  def getLookupPath(file: String) = if (isExist(file)) Option(getFullLookupPath(file)) else None

  // End section.

  def _getFullLookupPath(file: String) = otlconfig.getString("lookups.fs") + "//" + lookupFolder+ "/" + file

  def _getLookupPath(file: String) = Option(_getFullLookupPath(file))

  def merge(srcPath: String, dstPath: String) = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "file:/")

    val hdfs: FileSystem = {
      FileSystem.get(conf)
    }
    val src = new Path(srcPath)
    val dst = new Path(dstPath)
    if (hdfs.exists(dst)) hdfs.delete(dst, true)
    copyMerge(hdfs, src, hdfs, dst, deleteSource = true, conf)
  }

  def copyMerge(srcFS: FileSystem,
                srcDir: Path,
                dstFS: FileSystem,
                dstFile: Path,
                deleteSource: Boolean,
                conf: Configuration): Boolean = {

    if (dstFS.exists(dstFile))
      throw new IOException(s"Target $dstFile already exists")

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory) {

      val outputFile: FSDataOutputStream = dstFS.create(dstFile)
      Try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile =>
              val inputFile: FSDataInputStream = srcFS.open(status.getPath)
              Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
              inputFile.close()
          }
      }
      outputFile.close()

      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else false
  }

  def write(_df: DataFrame, fileName: String) = {
    val tmpFileName = fileName + ".tmp"
    _df.repartition(1)
      .write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save(tmpFileName)
    merge(tmpFileName, fileName)
  }
}
