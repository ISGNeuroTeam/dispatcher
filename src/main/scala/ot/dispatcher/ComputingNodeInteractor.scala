package ot.dispatcher

import java.util.UUID
import scala.sys.process._

class ComputingNodeInteractor(externalPort: Int) {
  val superKafkaConnector = new SuperKafkaConnector(externalPort)

  def registerNode(computingNodeUuid: UUID) = {
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
  }

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
  }
}
