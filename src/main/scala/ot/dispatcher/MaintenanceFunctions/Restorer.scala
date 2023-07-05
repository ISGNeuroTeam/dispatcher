package ot.dispatcher.MaintenanceFunctions

import ot.dispatcher.{CacheManager, CheckpointsManager, SuperDbConnector}

/** Consists of restoring DB, checkpoints and caches after reboot.
 *
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
object Restorer {

  /** Restores inconsistent state of DB after reboot.
   *
   * @param systemMaintenanceArgs Map with all args for different functions.
   */
  def restoreDB(systemMaintenanceArgs: Map[String, Any]): Unit = {
    val superConnector: SuperDbConnector = systemMaintenanceArgs("superConnector").asInstanceOf[SuperDbConnector]
    superConnector.interruptJobs()
    superConnector.clearCaches()
    superConnector.unlockCaches()
  }

  /** Removes all caches from RAM Cache directory.
   *
   * @param systemMaintenanceArgs Map with all args for different functions.
   */
  def restoreCacheDirectory(systemMaintenanceArgs: Map[String, Any]): Unit = {
    val cacheManager: CacheManager = systemMaintenanceArgs("cacheManager").asInstanceOf[CacheManager]
    cacheManager.clearCacheDirectory()
  }

  /**
   * Removes all content from checkpoints directory and set path to checkpoints directory in spark context
   * @param systemMaintenanceArgs Map with all args for different functions.
   */
  def restoreCheckpointsDirectory(systemMaintenanceArgs: Map[String, Any]): Unit = {
    val checkpointsManager: CheckpointsManager = systemMaintenanceArgs("checkpointsManager").asInstanceOf[CheckpointsManager]
    checkpointsManager.clearCheckpointsDirectory()
    checkpointsManager.setCheckpointsDirectory()
  }

}
