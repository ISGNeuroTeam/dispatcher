package ot.dispatcher

import ot.dispatcher.kafka.context.KafkaMessage
import play.api.libs.json.{JsValue, Json}
import sparkexecenv.CommandsProvider

/**
 * Contains functions, which create standard messages in computing node - Kafka broker connection
 */
object KafkaMessagesFactory {
  /**
   * Generate message for node registration
   * @param computingNodeUuid unique identifier of computing node
   * @param hostId host id defining through Java sys.process
   * @return target message
   */
  def createRegisterNodeMessage(computingNodeUuid: String, hostId: String, provider: CommandsProvider): KafkaMessage = {
    val commandName = "REGISTER_COMPUTING_NODE"
    val syntax: JsValue =  provider.getCommandSyntax
    val syntaxJson = Json.stringify(syntax)
    val registerMessage = {
      s"""
         |{
         |"computing_node_uuid": "${computingNodeUuid}",
         |"command_name": "${commandName}",
         |"command": {
         |    "computing_node_type": "SPARK",
         |    "host_id": "${hostId}",
         |    "otl_command_syntax": $syntaxJson,
         |    "resources": {
         |      "job_capacity": 999999999
         |    }
         |  }
         |}
         |""".stripMargin
    }
    createMessage("computing_node_control", commandName, registerMessage)
  }

  /**
   * Generate message for node unregistration
   * @param computingNodeUuid unique identifier of computing node
   * @return target message
   */
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

  /**
   * Generate message for resources state sending
   * @param computingNodeUuid unique identifier of computing node
   * @param activeExecutorsCount count of active spark executors
   * @return target message
   */
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

  /**
   * Generate message for error notifying
   * @param computingNodeUuid unique identifier of computing node
   * @param error error text
   * @return target message
   */
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

  /**
   * Generate message for job status notifying
   * @param jobUuid unique identifier of job
   * @param status status value from set {RUNNING, FINISHED, FAILED}
   * @param statusText text message, sending with status
   * @param lastFinishedCommand command, executed before current running command
   * @return target message
   */
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
