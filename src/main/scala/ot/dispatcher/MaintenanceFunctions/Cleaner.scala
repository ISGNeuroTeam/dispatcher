package ot.dispatcher.MaintenanceFunctions

import ot.dispatcher.{CacheManager, SuperConnector}

/** Removes cache files from RAM cache and DB rows.
 *
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
object Cleaner {

  def clearCache(systemMaintenanceArgs: Map[String, Any]): Unit = {

    val superConnector: SuperConnector = systemMaintenanceArgs("superConnector").asInstanceOf[SuperConnector]
    val cacheManager: CacheManager = systemMaintenanceArgs("cacheManager").asInstanceOf[CacheManager]
    val oldCaches = superConnector.deleteOldCache()
    while (oldCaches.next()) {
      val cacheId = oldCaches.getInt("id")
      cacheManager.removeCache(cacheId)
    }
  }
}
