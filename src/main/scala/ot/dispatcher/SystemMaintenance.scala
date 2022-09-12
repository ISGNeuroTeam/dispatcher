package ot.dispatcher

import ot.dispatcher.MaintenanceFunctions.Notifier

/** Runs System's maintenance.
 * 1. Clear expired caches.
 * 2. Cancel expired search jobs.
 *
 * @param systemMaintenanceArgs Map with all args for different functions.
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
class SystemMaintenance(systemMaintenanceArgs: Map[String, Any]) {

  def run(): Unit = {

    //Cleaner.clearCache(systemMaintenanceArgs)
    //Canceller.cancelJobs(systemMaintenanceArgs)
    //Tracker.keepAlive(systemMaintenanceArgs)
    if (systemMaintenanceArgs("kafkaExists").asInstanceOf[Boolean]) {
      Notifier.resourcesStateNotify(systemMaintenanceArgs)
    }
  }
}