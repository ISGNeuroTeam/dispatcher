package ot.scalaotl
package config

import java.io.File
import java.nio.file.Paths
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.DataFrame

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
    FileUtil.copyMerge(hdfs, src, hdfs, dst, true, conf, "")
  }

  def write(_df: DataFrame, fileName: String) = {
    val tmpFileName = fileName + ".tmp"
    _df.repartition(1)
      .write
      .format("csv")
      .option("ignoreTrailingWhiteSpace","false")
      .option("ignoreLeadingWhiteSpace","false")
      .option("header", "true")
      .mode("overwrite")
      .save(tmpFileName)
    merge(tmpFileName, fileName)
  }
}
