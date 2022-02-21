package ot.dispatcher.MaintenanceFunctions

import ot.dispatcher.{CacheManager, SuperConnector}

/** Consists of restoring DB and caches after reboot.
 *
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
object Restorer {

  /** Restores inconsistent state of DB after reboot.
   *
   * @param systemMaintenanceArgs Map with all args for different functions.
   */
  def restoreDB(systemMaintenanceArgs: Map[String, Any]): Unit = {
    val superConnector: SuperConnector = systemMaintenanceArgs("superConnector").asInstanceOf[SuperConnector]
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

}
