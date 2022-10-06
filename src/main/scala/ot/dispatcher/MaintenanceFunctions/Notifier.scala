package ot.dispatcher.MaintenanceFunctions

import org.apache.spark.sql.SparkSession
import ot.dispatcher.ComputingNodeInteractor

object Notifier {

  def resourcesStateNotify(systemMaintenanceArgs: Map[String, Any]): Unit = {
    val sparkSession: SparkSession = systemMaintenanceArgs("sparkSession").asInstanceOf[SparkSession]
    val nodeInteractor = systemMaintenanceArgs("nodeInteractor").asInstanceOf[ComputingNodeInteractor]
    val computingNodeUuid = systemMaintenanceArgs("computingNodeUuid").asInstanceOf[String]
    val sc = sparkSession.sparkContext
    val allExecutors = sc.getExecutorMemoryStatus.keys
    val driverHost: String = sc.getConf.get("spark.driver.host")
    val activeExecutorsCount = allExecutors.filter(!_.split(""":""")(0).equals(driverHost)).toList.size
    nodeInteractor.resourcesStateNotify(computingNodeUuid.toString, activeExecutorsCount)
  }

}