package ot.dispatcher.MaintenanceFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import ot.AppConfig._
import ot.dispatcher.SuperDbConnector

object Tracker {

  val log: Logger = Logger.getLogger("TrackerLogger")
  log.setLevel(Level.toLevel(getLogLevel(config, "tracker")))

  var lastCheck: Long = System.currentTimeMillis() / 1000
  val checkInterval: Int = config.getInt("tracker.interval")

  def keepAlive(systemMaintenanceArgs: Map[String, Any]): Unit = {
    val nowTime = System.currentTimeMillis() / 1000
    if (nowTime > lastCheck + checkInterval) {
      log.trace(s"System time: $nowTime, Last check: $lastCheck.")
      val superConnector: SuperDbConnector = systemMaintenanceArgs("superConnector").asInstanceOf[SuperDbConnector]
      val sparkSession: SparkSession = systemMaintenanceArgs("sparkSession").asInstanceOf[SparkSession]
      lastCheck = superConnector.tick(sparkSession.sparkContext.applicationId)
    }
  }

  def registerDispatcher(restorationMaintenanceArgs: Map[String, Any]): Unit = {
    val superConnector: SuperDbConnector = restorationMaintenanceArgs("superConnector").asInstanceOf[SuperDbConnector]
    val sparkSession: SparkSession = restorationMaintenanceArgs("sparkSession").asInstanceOf[SparkSession]
    lastCheck = superConnector.firstTick(sparkSession.sparkContext.applicationId)
    log.info(s"Register SuperDispatcher (${sparkSession.sparkContext.applicationId}) at $lastCheck.")
  }

}
