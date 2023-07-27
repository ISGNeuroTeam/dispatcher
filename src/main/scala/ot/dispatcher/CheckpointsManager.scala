package ot.dispatcher

import com.typesafe.config.ConfigException
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import ot.AppConfig._

import java.io.File
import java.net.URI

/**
 * Makes all manipulations with dataframes checkpoints directory
 * [[clearCheckpointsDirectory]] - remove all content from directory
 * [[setCheckpointsDirectory]] - set path to directory in spark context
 * @param sparkSession current spark session
 */
class CheckpointsManager(sparkSession: SparkSession) {
  val log: Logger = Logger.getLogger("CheckpointsManagerLogger")
  log.setLevel(Level.toLevel(getLogLevel(config, "checkpointsmanager")))

  // Loads settings for checkpoints from config.
  val path: String = try {
    config.getString("checkpoints.path")
  } catch {
    case ex: ConfigException.Missing => "/opt/otp/dispatcher/checkpoints"
  }

  /**
   * Remove all content from directory
   */
  def clearCheckpointsDirectory(): Unit = {
    if (path.nonEmpty) {
      val fs = org.apache.hadoop.fs.FileSystem.get(new URI(path), sparkSession.sparkContext.hadoopConfiguration)
      fs.delete(new Path(path), true)
    }
  }

  /**
   * Set path to directory in spark context
   */
  def setCheckpointsDirectory(): Unit = {
    if (config.getString("checkpoints.enabled") != "onlyFalse") {
      log.info(s"Checkpoints directory setting: $path")
      val dir = new File(path)
      if (!dir.exists())
        dir.mkdirs()
      sparkSession.sparkContext.setCheckpointDir(path)
    }
  }

}
