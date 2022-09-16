package ot.dispatcher

import org.apache.log4j.{Level, Logger}
import ot.AppConfig.{config, getLogLevel}

import java.util.UUID
import scala.sys.process._

/**
 * Provides functional API for spark computing node interaction with Kafka
 * @param ipAddress - address of node where kafka service hosted
 * @param externalPort - kafka service port
 */
class ComputingNodeInteractor(val ipAddress: String, val externalPort: Int) {
  val log: Logger = Logger.getLogger("NodeInteractorLogger")
  log.setLevel(Level.toLevel(getLogLevel(config, "node_interactor")))

  /**
   * Instance of inner connector to Kafka
   */
  val superKafkaConnector = new SuperKafkaConnector(ipAddress, externalPort)

  /**
   * Send registration message with information about spark computing node to Kafka
   * @param computingNodeUuid - unique identifier of computing node
   * @return
   */
  def registerNode(computingNodeUuid: UUID) = {
    //host id defining through Java sys.process
    val p = Process("hostid")
    val hostId: String = p.!!.trim()

    val commandName = "REGISTER_COMPUTING_NODE"
    val registerMessage =
      s"""
         |{
         |"computing_node_uuid": "${computingNodeUuid}",
         |"command_name": "${commandName}",
         |"command": {
         |    "computing_node_type": "SPARK",
         |    "host_id": "${hostId}",
         |    "otl_command_syntax": {},
         |    "resources": {
         |      "job_capacity": 999999999
         |    }
         |  }
         |}
         |""".stripMargin
    superKafkaConnector.sendMessage("computing_node_control", commandName, registerMessage)
    log.info(s"Registering Node with ID ${computingNodeUuid}, Host ID: ${hostId}")
  }

  /**
   * Send spark computing node's unregistration message to Kafka
   * @param computingNodeUuid - unique identifier of computing node
   * @return
   */
  def unregisterNode(computingNodeUuid: UUID) = {
    val commandName = "UNREGISTER_COMPUTING_NODE"
    val unregisterMessage =
      s"""
         |{
         |"computing_node_uuid": "${computingNodeUuid}",
         |"command_name": "${commandName}",
         |"command": {}
         |}
         |""".stripMargin
    superKafkaConnector.sendMessage("computing_node_control", commandName, unregisterMessage)
    log.info(s"Unregistering Node with ID ${computingNodeUuid}")
  }

  def launchJobsGettingProcess(computingNodeUuid: UUID) = {
    new Thread() {
      override def run(): Unit = superKafkaConnector.getNewJobs(computingNodeUuid.toString)
    }.start()
  }
}