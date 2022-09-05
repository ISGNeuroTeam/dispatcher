package ot.dispatcher.MaintenanceFunctions

import ot.dispatcher.{CacheManager, SuperDbConnector}

/** Removes cache files from RAM cache and DB rows.
 *
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
object Cleaner {

  def clearCache(systemMaintenanceArgs: Map[String, Any]): Unit = {

    val superConnector: SuperDbConnector = systemMaintenanceArgs("superConnector").asInstanceOf[SuperDbConnector]
    val cacheManager: CacheManager = systemMaintenanceArgs("cacheManager").asInstanceOf[CacheManager]
    val oldCaches = superConnector.deleteOldCache()
    while (oldCaches.next()) {
      val cacheId = oldCaches.getInt("id")
      cacheManager.removeCache(cacheId)
    }
  }
}
