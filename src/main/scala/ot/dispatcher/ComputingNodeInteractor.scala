package ot.dispatcher

import org.apache.log4j.{Level, Logger}
import ot.AppConfig.{config, getLogLevel}

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

  def registerNode(computingNodeUuid: String, hostId: String) = {
    val message = KafkaMessagesFactory.createRegisterNodeMessage(computingNodeUuid, hostId)
    superKafkaConnector.sendMessage(message)
  }

  def unregisterNode(computingNodeUuid: String) = {
    val message = KafkaMessagesFactory.createUnregisterNodeMessage(computingNodeUuid)
    superKafkaConnector.sendMessage(message)
  }

  def resourcesStateNotify(computingNodeUuid: String, activeExecutorsCount: Int) = {
    val message = KafkaMessagesFactory.createResourcesStateNotifyMessage(computingNodeUuid, activeExecutorsCount)
    superKafkaConnector.sendMessage(message)
  }

  def errorNotify(computingNodeUuid: String, error: String) = {
    val message = KafkaMessagesFactory.createErrorNotifyMessage(computingNodeUuid, error)
    superKafkaConnector.sendMessage(message)
  }

  def jobStatusNotify(jobUuid: String, status: String, statusText: String, lastFinishedCommand: String = "") = {
    val message = KafkaMessagesFactory.createJobStatusNotifyMessage(jobUuid, status, statusText, lastFinishedCommand)
    superKafkaConnector.sendMessage(message)
  }

  def logProgressMessage(jobUuid: String, message: String, commandName: String, commandIndexInPipeline: Int, totalCommandsInPipeline: Int, stage: Int, totalStages: Int, depth: Int) = {
    val statusMessage = formatLogProgressMessage(message, commandName, commandIndexInPipeline, totalCommandsInPipeline, stage, totalStages, depth)
    jobStatusNotify(jobUuid, "RUNNING", statusMessage)
  }

  private def formatLogProgressMessage(message: String, commandName: String, commandIndexInPipeline: Int, totalCommandsInPipeline: Int, stage: Int, totalStages: Int, depth: Int) = {
    val commandMessage = "\t"*depth + s"${commandIndexInPipeline} out of ${totalCommandsInPipeline} command. Command ${commandName}"
    val stageMessage = if (stage == 0) {
      ""
    } else {
      s"${stage}/${totalStages}"
    }
    commandMessage + " " + stageMessage + " " + message
  }

  def launchJobsGettingProcess(computingNodeUuid: String) = {
    new Thread() {
      override def run(): Unit = superKafkaConnector.getNewJobs(computingNodeUuid)
    }.start()
  }

}