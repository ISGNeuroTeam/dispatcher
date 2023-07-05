package ot.dispatcher

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import ot.AppConfig._

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
  val path: String = config.getString("spark.path_checkpoint")

  /**
   * Remove all content from directory
   */
  def clearCheckpointsDirectory(): Unit = {
    val fs = org.apache.hadoop.fs.FileSystem.get(new URI(path), sparkSession.sparkContext.hadoopConfiguration)
    fs.delete(new Path(path), true)
  }

  /**
   * Set path to directory in spark context
   */
  def setCheckpointsDirectory(): Unit = {
    sparkSession.sparkContext.setCheckpointDir(path)
  }

}
