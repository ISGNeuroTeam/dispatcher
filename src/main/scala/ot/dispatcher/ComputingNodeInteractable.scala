package ot.dispatcher

import com.isgneuro.sparkexecenv.CommandsProvider

trait ComputingNodeInteractable {
  /**
   * Send registration message with information about spark computing node to Kafka
   *
   * @param computingNodeUuid unique identifier of computing node
   * @param hostId            host id defining through Java sys.process
   */
  def registerNode(computingNodeUuid: String, hostId: String, provider: CommandsProvider): Unit

  /**
   * Send spark computing node's unregistration message to Kafka
   *
   * @param computingNodeUuid unique identifier of computing node
   */
  def unregisterNode(computingNodeUuid: String): Unit

  /**
   * Send information about used resources
   *
   * @param computingNodeUuid    unique identifier of computing node
   * @param activeExecutorsCount count of active spark executors
   */
  def resourcesStateNotify(computingNodeUuid: String, activeExecutorsCount: Int): Unit
  /**
   * Sending message about error on computing node
   *
   * @param computingNodeUuid unique identifier of computing node
   * @param error             error text
   */
  def errorNotify(computingNodeUuid: String, error: String): Unit

  /**
   * Send job status
   *
   * @param jobUuid             unique identifier of job
   * @param status              status value from set {RUNNING, FINISHED, FAILED}
   * @param statusText          text message, sending with status
   * @param lastFinishedCommand command, executed before current running command
   */
  def jobStatusNotify(jobUuid: String, status: String, statusText: String, lastFinishedCommand: String = ""): Unit

  /**
   * Function for computing node to Kafka dispatcher logging
   *
   * @param jobUuid                 unique identifier of job
   * @param message                 info message
   * @param commandName             name of current command
   * @param commandIndexInPipeline  index number of command in commands pipeline
   * @param totalCommandsInPipeline total count of commands in pipeline
   * @param stage                   current stage
   * @param totalStages             total count of stages
   * @param depth                   depth of subsearch
   */
  def logProgressMessage(jobUuid: String, message: String, commandName: String, commandIndexInPipeline: Int, totalCommandsInPipeline: Int, stage: Int, totalStages: Int, depth: Int): Unit

  /**
   * Function, which start jobs getting from Kafka in separate thread
   *
   * @param computingNodeUuid
   */
  def launchJobsGettingProcess(computingNodeUuid: String): Unit

}
