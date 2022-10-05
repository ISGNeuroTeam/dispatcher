package ot.dispatcher

import ot.dispatcher.MaintenanceFunctions.{Restorer, Tracker}

/** Restores DB and RAM Cache after reboot or on first run.
 * 1. Check DB scheme.
 * 2. Creates DB scheme if it is empty.
 * 3. Updates unfinished jobs with failed state.
 * 4. Remove caches from RAM Cache.
 *
 * @param restorationMaintenanceArgs Map with all args for different functions.
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
class RestorationMaintenance(restorationMaintenanceArgs: Map[String, Any]) {

  def run(): Unit = {

    Restorer.restoreDB(restorationMaintenanceArgs)
    Restorer.restoreCacheDirectory(restorationMaintenanceArgs)
    Tracker.registerDispatcher(restorationMaintenanceArgs)
  }
}