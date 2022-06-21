package ot.scalaotl
package utils
package searchinternals

import ot.scalaotl.config.OTLIndexes
import org.apache.hadoop.fs.{ FileSystem, Path }
import scala.collection.mutable.ListBuffer
import org.apache.log4j.{ Level, Logger }
import ot.AppConfig.config
import ot.AppConfig.getLogLevel

object Timerange extends OTLIndexes {

  val classname: String = this.getClass.getSimpleName
  val log: Logger = Logger.getLogger(classname)
  log.setLevel(Level.toLevel(getLogLevel(config, "Timerange")))

  private def getBucketsFromFs(fs: FileSystem, INDEX_PATH: String, index: String, isCache: Boolean = false): ListBuffer[String] =
  {
    val filenames = ListBuffer[String]()
    try {
      var status = fs.listStatus(new Path(INDEX_PATH + index))
      val timestamp: Long = System.currentTimeMillis
      if (isCache) {
        log.debug("Filter buckets by modification time")
        status = status.filter(x =>
        {
          log.debug(s"$x difference: " + (timestamp - x.getModificationTime).toString)
          (timestamp - x.getModificationTime) < duration_cache * 1000
        })
      }
      status.foreach(x => filenames += x.getPath.getName)
    } catch { case e: Exception => log.debug(e); return filenames}
    filenames
  }

  private def isBucketInTimerange(bucket: String, tws: Long, twf: Long): Boolean =
  {
    try{
      val t_array = bucket.split("-")
      val t1 = t_array(1).toLong
      val t2 = t_array(2).toLong

      if ((t1 <= twf) && (t2 >= tws)) {
        return true
      }
   } catch { case e: Exception => log.debug(s"Exception $e in $bucket, remove from search")}
      false
  }

  def getBucketsByTimerange(fs: FileSystem, INDEX_PATH: String, index: String, tws: Long, twf: Long, isCache: Boolean): ListBuffer[String] = {
    val filenames = getBucketsFromFs(fs, INDEX_PATH, index, isCache)
    log.debug(s"Timerange: [$tws, $twf]")
    val rfilenames = filenames.filter(isBucketInTimerange(_, tws, twf))
    rfilenames
  }
}
