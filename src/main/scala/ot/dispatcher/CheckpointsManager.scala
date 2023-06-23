package ot.dispatcher

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import ot.AppConfig._

class CheckpointsManager(sparkSession: SparkSession) {
  val log: Logger = Logger.getLogger("CheckpointsManagerLogger")

  // Loads settings for checkpoints from config.
  val fs: String = config.getString("spark.fs_checkpoint")
  val path: String = config.getString("spark.path_checkpoint")

  def setCheckpointsDir(): Unit = {
    sparkSession.sparkContext.setCheckpointDir(path)
  }

}
