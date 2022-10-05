package ot.dispatcher

import ot.dispatcher.kafka.context.KafkaMessage

object KafkaMessagesFactory {
  def createRegisterNodeMessage(computingNodeUuid: String, hostId: String): KafkaMessage = {
    val commandName = "REGISTER_COMPUTING_NODE"
    val registerMessage = {
      s"""
         |{
         |"computing_node_uuid": "${computingNodeUuid}",
         |"command_name": "${commandName}",
         |"command": {
         |    "computing_node_type": "SPARK",
         |    "host_id": "${hostId}",
         |    "otl_command_syntax": {
         |      "test": {
         |        "rules": [
         |          {
         |            "name": "arg1",
         |            "type": "kwarg",
         |            "key": "num",
         |            "inf": false,
         |            "required": true,
         |            "input_types": ["string"]
         |          }
         |        ]
         |      }
         |    },
         |    "resources": {
         |      "job_capacity": 999999999
         |    }
         |  }
         |}
         |""".stripMargin
    }
    createMessage("computing_node_control", commandName, registerMessage)
  }

  def createUnregisterNodeMessage(computingNodeUuid: String): KafkaMessage = {
    val commandName = "UNREGISTER_COMPUTING_NODE"
    val unregisterMessage =
      s"""
         |{
         |"computing_node_uuid": "${computingNodeUuid}",
         |"command_name": "${commandName}",
         |"command": {}
         |}
         |""".stripMargin
    createMessage("computing_node_control", commandName, unregisterMessage)
  }

  def createResourcesStateNotifyMessage(computingNodeUuid: String, activeExecutorsCount: Int): KafkaMessage = {
    val commandName = "RESOURCE_STATUS"
    val resourceStatusMessage =
      s"""
         |{
         |"computing_node_uuid": "${computingNodeUuid}",
         |"command_name": "${commandName}",
         |"command": {
         |    "resources": {
         |      "job_capacity": ${activeExecutorsCount.toString}
         |    }
         |  }
         |}
         |""".stripMargin
    createMessage("computing_node_control", commandName, resourceStatusMessage)
  }

  def createErrorNotifyMessage(computingNodeUuid: String, error: String): KafkaMessage = {
    val commandName = "ERROR_OCCURED"
    val errorMessage =
      s"""
         |{
         |"computing_node_uuid": "${computingNodeUuid}",
         |"command_name": "${commandName}",
         |"command": {
         |    "error": "${error}"
         |  }
         |}
         |""".stripMargin
    createMessage("computing_node_control", commandName, errorMessage)
  }

  def createJobStatusNotifyMessage(jobUuid: String, status: String, statusText: String, lastFinishedCommand: String) = {
    val message = if (lastFinishedCommand.isEmpty) {
      s"""
         |{
         |"uuid": "${jobUuid}",
         |"status": "${status}",
         |"status_text": "${statusText}"
         |}
         |""".stripMargin
    } else {
      s"""
         |{
         |"uuid": "${jobUuid}",
         |"status": "${status}",
         |"status_text": "${statusText}",
         |"last_finished_command": "${lastFinishedCommand}"
         |}
         |""".stripMargin
    }
    createMessage("nodejob_status", "JOB_STATUS_NOTIFY", message)
  }

  private def createMessage(topic: String, key: String, value: String): KafkaMessage = {
    KafkaMessage(topic, key, value)
  }
}
