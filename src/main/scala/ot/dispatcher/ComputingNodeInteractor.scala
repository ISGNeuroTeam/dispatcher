package ot.dispatcher

import org.apache.log4j.{Level, Logger}
import ot.AppConfig.{config, getLogLevel}
import sparkexecenv.CommandsProvider

/**
 * Provides functional API for spark computing node interaction with Kafka
 * @param ipAddress address of node where kafka service hosted
 * @param externalPort kafka service port
 */
class ComputingNodeInteractor(val ipAddress: String, val externalPort: Int) extends ComputingNodeInteractable {
  val log: Logger = Logger.getLogger("NodeInteractorLogger")
  log.setLevel(Level.toLevel(getLogLevel(config, "node_interactor")))

  /**
   * Instance of inner connector to Kafka
   */
  val superKafkaConnector = new SuperKafkaConnector(ipAddress, externalPort)

  /**
   * Send registration message with information about spark computing node to Kafka
   * @param computingNodeUuid unique identifier of computing node
   * @param hostId host id defining through Java sys.process
   */
  def registerNode(computingNodeUuid: String, hostId: String, provider: CommandsProvider) = {
    val message = KafkaMessagesFactory.createRegisterNodeMessage(computingNodeUuid, hostId, provider)
    superKafkaConnector.sendMessage(message)
  }

  /**
   * Send spark computing node's unregistration message to Kafka
   * @param computingNodeUuid unique identifier of computing node
   */
  def unregisterNode(computingNodeUuid: String) = {
    val message = KafkaMessagesFactory.createUnregisterNodeMessage(computingNodeUuid)
    superKafkaConnector.sendMessage(message)
  }

  /**
   * Send information about used resources
   * @param computingNodeUuid unique identifier of computing node
   * @param activeExecutorsCount count of active spark executors
   */
  def resourcesStateNotify(computingNodeUuid: String, activeExecutorsCount: Int) = {
    val message = KafkaMessagesFactory.createResourcesStateNotifyMessage(computingNodeUuid, activeExecutorsCount)
    superKafkaConnector.sendMessage(message)
  }

  /**
   * Sending message about error on computing node
   * @param computingNodeUuid unique identifier of computing node
   * @param error error text
   */
  def errorNotify(computingNodeUuid: String, error: String) = {
    val message = KafkaMessagesFactory.createErrorNotifyMessage(computingNodeUuid, error)
    superKafkaConnector.sendMessage(message)
  }

  /**
   * Send job status
   * @param jobUuid unique identifier of job
   * @param status status value from set {RUNNING, FINISHED, FAILED}
   * @param statusText text message, sending with status
   * @param lastFinishedCommand command, executed before current running command
   */
  def jobStatusNotify(jobUuid: String, status: String, statusText: String, lastFinishedCommand: String = "") = {
    val message = KafkaMessagesFactory.createJobStatusNotifyMessage(jobUuid, status, statusText, lastFinishedCommand)
    superKafkaConnector.sendMessage(message)
  }

  /**
   * Function for computing node to Kafka dispatcher logging
   * @param jobUuid unique identifier of job
   * @param message info message
   * @param commandName name of current command
   * @param commandIndexInPipeline index number of command in commands pipeline
   * @param totalCommandsInPipeline total count of commands in pipeline
   * @param stage current stage
   * @param totalStages total count of stages
   * @param depth depth of subsearch
   */
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

  /**
   * Function, which start jobs getting from Kafka in separate thread
   * @param computingNodeUuid
   */
  def launchJobsGettingProcess(computingNodeUuid: String) = {
    new Thread() {
      override def run(): Unit = superKafkaConnector.getNewJobs(computingNodeUuid)
    }.start()
  }

}