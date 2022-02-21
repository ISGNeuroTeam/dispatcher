package ot.dispatcher

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import ot.AppConfig._
import ot.scalaotl.Converter
import ot.dispatcher.sdk.core.CustomException.{E00008, E00009, E00010}

/** Represents the start point of main calculation process for Job.
 *
 * @param cacheManager   [[CacheManager]] instance for manipulations with RAM cache.
 * @param superConnector [[SuperVisor]] instance for DB updates depending on Job states.
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
class SuperCalculator(cacheManager: CacheManager, superConnector: SuperConnector) {

  val log: Logger = Logger.getLogger("CalculatorLogger")
  log.setLevel(Level.toLevel(getLogLevel(config, "calculator")))

  /** Calculation conveyor.
   * 1. Gets Map of Subsearches' cache [[DataFrame]]s if they present and lock them to prevent ttl removing.
   * 2. Makes calculation.
   * 3. Save result [[DataFrame]] of calculations to RAM cache.
   * 4. Unlock subsearches' caches.
   *
   * @param otlQuery [[OTLQuery]] Job data instance.
   */
  def calc(otlQuery: OTLQuery): Unit = {
    log.debug(s"Job ${otlQuery.id}. Calculations are in progress.")

    // Loads and locks subsearches' caches.
    val (cache: Map[String, DataFrame], lockedCaches: List[Int]) = getSubsearchesCache(otlQuery)
    log.debug(s"Job ${otlQuery.id}. Caches are loaded.")
    log.debug(s"Job ${otlQuery.id}. Caches: $cache")
    log.debug(s"Job ${otlQuery.id}. Caches.keys: ${cache.keys}")

    // Starts calculations.
    val rdf: DataFrame = new Converter(otlQuery, cache).run
    log.debug(s"Job ${otlQuery.id}. Saving results..")

    // Saves result cache.
    cacheManager.makeCache(rdf, otlQuery.id)
    log.debug(s"Job ${otlQuery.id}. Results are saved.")

    // Unlock subsearches' caches.
    lockedCaches.foreach(cacheId => {
      superConnector.unlockCache(cacheId, otlQuery.id)
    })
    log.debug(s"Job ${otlQuery.id}. Caches $lockedCaches are unlocked.")
  }

  /** Calculates hash for subsearch ID based on it's query.
   *
   * @param text subsearch query.
   * @return sha256 hash [[String]].
   */
  def sha256Hash(text: String): String = String.format(
    "%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256")
      .digest(text.trim.getBytes("UTF-8"))
    )
  )

  /** Returns Map with subsearches' caches [[DataFrame]]s and List of IDs for locking.
   *
   * 1. Waits for IDs of registered subsearches' caches.
   * 2. Locks subsearches' caches.
   * 3. Waits for ready state of all caches.
   * 4. Loads caches to [[DataFrame]]s.
   *
   * @param otlQuery [[OTLQuery]] Job data.
   * @return caches.
   */
  def getSubsearchesCache(otlQuery: OTLQuery): (Map[String, DataFrame], List[Int]) = {

    log.debug(s"Job ${otlQuery.id}. Checking for subsearches.")
    var dfs = Map[String, DataFrame]()

    var lockedCaches = List[Int]()

    // Step 1. Waits for IDs of registered subsearches' caches.
    while (lockedCaches.length < otlQuery.subsearches.size) {

      otlQuery.subsearches.values.foreach(subsearch => {
        log.trace(s"Looking for subsearch: $subsearch.")
        val res = superConnector.getSubsearchID(subsearch)
        // Fake id of cache if it is not registered.
        var subsearchId: Int = -1
        while (res.next()) {
          subsearchId = res.getInt("id")
          if (!lockedCaches.contains(subsearchId)) {
            lockedCaches = subsearchId :: lockedCaches
          }
        }
      })
    }

    log.debug(s"Caches which will be locked: ${lockedCaches.mkString(",")}")
    // Step 2. Locks subsearches' caches.
    lockedCaches.foreach(cacheId => {
      superConnector.lockCache(cacheId, otlQuery.id)
      log.debug(s"Job ${otlQuery.id}. Cache $cacheId is locked.")
    })

    log.debug(s"Job ${otlQuery.id}. Caches $lockedCaches are locked.")

    // Step 3. Waits for ready state of all caches.
    var cacheReadyFlag = false
    while (!cacheReadyFlag) {

      cacheReadyFlag = true

      val subsearches = otlQuery.subsearches.values.map(subsearch => {
        val res = superConnector.getSubsearchStatus(subsearch)
        // Because of caches are looking for depending on their original_otl we need to wait while all of them will be
        // registered and then to get it's id.
        var subsearchId: Int = -1 // Fake id of cache if it is not registered.
        while (res.next()) {
          val status = res.getString("status")
          log.trace(s"Job ${otlQuery.id}. Subsearch ($subsearch) status is $status.")
          subsearchId = res.getInt("id")
          status match {
            case "new" => cacheReadyFlag = false
            case "running" => cacheReadyFlag = false
            case "finished" => log.trace(s"Job ${otlQuery.id}. Subsearch ($subsearch) finished.")
            case "external" =>
            case "failed" => throw E00008(otlQuery.id)
            case "canceled" => throw E00009(otlQuery.id)
            case _ => throw E00010(otlQuery.id)
          }
        }
        subsearchId match {
          case -1 => cacheReadyFlag = false
          case _ => log.trace(s"Job ${otlQuery.id}. Subsearch ($subsearch) is not yet ready.")
        }
        (subsearch, subsearchId)
      })

      if (cacheReadyFlag) {
        log.debug(s"Job ${otlQuery.id}. Subsearches are ready. Loading.")
        // Loads caches.
        subsearches.foreach({ subsearch =>
          // Parses endpoint if command is "otrest".
          val cmd = "otrest[^|]+url\\s*?=\\s*?[^|\\] ]+".r.findFirstMatchIn(subsearch._1)
          // Calculates subsearch ID
          val sid = cmd match {
            // If command is "otrest".
            case Some(ep) => s"subsearch_${sha256Hash(ep.group(0))}"
            // If subsearch is general.
            case None => s"subsearch_${sha256Hash(subsearch._1)}"
          }
          val id = subsearch._2
          dfs = dfs + (sid -> cacheManager.loadCache(id))
          log.debug(s"Job ${otlQuery.id}. Subsearch $subsearch is loaded. SID: $sid. ID: $id.")
        })
      }
    }
    log.debug(s"Job ${otlQuery.id}. DFs: $dfs")
    (dfs, lockedCaches)
  }
}
