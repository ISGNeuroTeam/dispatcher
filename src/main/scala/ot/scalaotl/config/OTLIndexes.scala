package ot.scalaotl
package config

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.collection.mutable.ListBuffer

trait OTLIndexes extends OTLConfig {
 
  val fsdisk: String = otlconfig.getString("indexes.fs_disk")
  val indexPathDisk: String = "//" + new File(otlconfig.getString("indexes.path_disk")).getCanonicalPath + "/"
  val fscache: String = otlconfig.getString("indexes.fs_cache")
  val indexPathCache: String = "//" +  new File(otlconfig.getString("indexes.path_cache")).getCanonicalPath + "/"
  val duration_cache: Long = otlconfig.getString("indexes.duration_cache").toLong
  val max_cols: Int = otlconfig.getString("indexes.max_cols").toInt
  val max_mv_size: Int = otlconfig.getString("indexes.max_mv_size").toInt

  val fs_disk: FileSystem = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", fsdisk)
    FileSystem.get(conf)
  }

  val fs_cache: FileSystem = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", fscache)
    FileSystem.get(conf)
  }
  
  def getAllIndexes():ListBuffer[String] = {
    val indexes_name = ListBuffer[String]() 
    fs_disk.listStatus(new Path(indexPathDisk)).map(x => indexes_name += x.getPath.getName)
    indexes_name
  }
}
