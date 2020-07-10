package ot.dispatcher.MaintenanceFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import ot.AppConfig._
import ot.dispatcher.SuperConnector

/** Removes cache files from RAM cache and DB rows.
  *
  * @author Andrey Starchenkov (astarchenkov@ot.ru)
  */
object Canceller {

  val log: Logger = Logger.getLogger("CancellerLogger")
  log.setLevel(Level.toLevel(getLogLevel(config,"canceller")))


  def cancelJobs(systemMaintenanceArgs: Map[String, Any]): Unit = {

    val superConnector: SuperConnector = systemMaintenanceArgs("superConnector").asInstanceOf[SuperConnector]
    val sparkSession: SparkSession = systemMaintenanceArgs("sparkSession").asInstanceOf[SparkSession]
    val expiredJobs = superConnector.getExpiredQueries
    while (expiredJobs.next()) {
      val jobID = expiredJobs.getInt("id")
      log.info(s"Job $jobID was cancelled because of timeout.")
      sparkSession.sparkContext.cancelJobGroup(s"Search ID $jobID")
      superConnector.setJobStateCanceled(jobID)
      superConnector.unlockCaches(jobID)
    }
  }
}
