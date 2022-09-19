package ot.dispatcher.MaintenanceFunctions

import org.apache.spark.sql.SparkSession
import ot.dispatcher.SuperKafkaConnector

import java.util.UUID

object Notifier {

  def resourcesStateNotify(systemMaintenanceArgs: Map[String, Any]): Unit = {
    val sparkSession: SparkSession = systemMaintenanceArgs("sparkSession").asInstanceOf[SparkSession]
    val kafkaConnector = systemMaintenanceArgs("kafkaConnector").asInstanceOf[SuperKafkaConnector]
    val computingNodeUuid = systemMaintenanceArgs("computingNodeUuid").asInstanceOf[UUID]
    val sc = sparkSession.sparkContext
    val allExecutors = sc.getExecutorMemoryStatus.keys
    val driverHost: String = sc.getConf.get("spark.driver.host")
    val activeExecutorsCount = allExecutors.filter(!_.split(""":""")(0).equals(driverHost)).toList.size
    val commandName = "RESOURCE_STATUS"
    val resourceStatusMessage =
      s"""
         |{
         |"computing_node_uuid": "${computingNodeUuid}",
         |"command_name": "${commandName}"
         |"command": {
         |    "resources": {
         |      "job_capacity": ${activeExecutorsCount.toString}
         |    }
         |  }
         |}
         |""".stripMargin
    kafkaConnector.sendMessage("computing_node_control", commandName, resourceStatusMessage)
  }

}