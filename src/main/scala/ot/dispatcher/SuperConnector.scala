package ot.dispatcher

import java.sql.{Connection, DriverManager, ResultSet}
import ot.AppConfig.config

/** Provides all operations with DB.
  * [[getDBConnection]] - Returns connection to DB.
  * == Job Section ==
  * [[getNewQueries]] - Returns all new Jobs.
  * [[setJobStateRunning]] - Sets Job state to running.
  * [[setJobStateFinished]] - Sets Job state to finished.
  * [[setJobStateFailed]] - Sets Job state to failed.
  * == Cache Section ==
  * [[deleteOldCache]] - Removes old cache (depending on expire date an lock status) rows from DB.
  * [[addNewCache]] - Registers new Job calculated cache.
  * [[lockCache]] - Sets lock on Job cache removing because of subsearches.
  * [[unlockCache]] - Removes lock on Job cache removing.
  * == Subsearch section ==
  * [[getSubsearchStatus]] - Returns subsearch Job ID.
  * == Restoration Section ==
  * [[interruptJobs]] - Set all running or new jobs to failed state.
  * [[clearCaches]] - Deletes all caches.
  * [[unlockCaches]] - Unlocks all caches.
  * [[sql_without_results]] - Uses for creating statements if DB scheme is empty.
  *
  * @author Andrey Starchenkov (astarchenkov@ot.ru)
  */
class SuperConnector {

  private val dbConnection = getDBConnection
  private val jobTimeout = config.getString("searches.timeout")

  /** Returns connection to DB. */
  def getDBConnection: Connection = {
    //    var connection:Connection = null
    Class.forName(config.getString("jdbc.driver"))
    val connection = DriverManager.getConnection(config.getString("jdbc.url"),
      config.getString("jdbc.username"), config.getString("jdbc.password"))
    connection
  }

  /** Returns all new Jobs. */
  def getNewQueries: ResultSet = {
    val stm_select_queries = dbConnection.prepareStatement(
      "SELECT OTLQueries.id, OTLQueries.original_otl, OTLQueries.service_otl, OTLQueries.subsearches, OTLQueries.tws, OTLQueries.twf, " +
        "OTLQueries.cache_ttl, OTLQueries.username, RoleModel.indexes, OTLQueries.field_extraction, OTLQueries.preview FROM OTLQueries " +
        "LEFT JOIN RoleModel ON OTLQueries.username = RoleModel.username WHERE OTLQueries.status = 'new';"
    )
    val res = stm_select_queries.executeQuery()
    res
  }

  /** Sets Job state to running. */
  def setJobStateRunning(id: Int): Int = {
    val stm_upd_running = dbConnection.prepareStatement(s"UPDATE OTLQueries SET status = 'running' WHERE id = $id;")
    stm_upd_running.executeUpdate()
  }

  /** Sets Job state to finished. */
  def setJobStateFinished(id: Int): Int = {
    val stm_upd_finished = dbConnection.prepareStatement(s"UPDATE OTLQueries SET status = 'finished' WHERE id = $id;")
    stm_upd_finished.executeUpdate()
  }

  /** Sets Job state to failed. */
  def setJobStateFailed(id: Int, msg: String): Int = {
    val stm_upd_failed = dbConnection.prepareStatement(s"UPDATE OTLQueries SET status = 'failed', msg = $$SuperToken$$$msg$$SuperToken$$ WHERE id = $id;")
    stm_upd_failed.executeUpdate()
  }

  /** Removes old cache (depending on expire date an lock status) rows from DB. */
  def deleteOldCache(): ResultSet = {
    val stm_del_cache = dbConnection.prepareStatement(s"DELETE from CachesDL WHERE expiring_date  < CURRENT_TIMESTAMP " +
      s"AND id NOT IN (SELECT id FROM CachesLock) " +
      s"RETURNING id;")
    val res = stm_del_cache.executeQuery()
    res
  }

  /** Registers new Job calculated cache. */
  def addNewCache(otlQuery: OTLQuery): Unit = {
    val stm_ins_cache = dbConnection.prepareStatement(s"INSERT INTO CachesDL (original_otl, tws, twf, id, expiring_date, field_extraction, preview)" +
      s" VALUES($$SuperToken$$${otlQuery.original_otl}$$SuperToken$$, ${otlQuery.tws}, ${otlQuery.twf}, ${otlQuery.id}," +
      s" to_timestamp(extract(epoch from now()) + ${otlQuery.cache_ttl}), ${otlQuery.field_extraction}, ${otlQuery.preview})")
    stm_ins_cache.execute()
  }

  /** Returns subsearch Job ID and status. */
  def getSubsearchID(original_otl: String): ResultSet = {
    val stm_select_cache_id = dbConnection.prepareStatement(
      s"SELECT id FROM OTLQueries WHERE original_otl = $$SuperToken$$$original_otl$$SuperToken$$ ORDER BY id DESC LIMIT 1;"
    )
    val res = stm_select_cache_id.executeQuery()
    res
  }

  /** Returns subsearch Job ID and status. */
  def getSubsearchStatus(original_otl: String): ResultSet = {
    val stm_select_cache_id = dbConnection.prepareStatement(
      s"SELECT id, status FROM OTLQueries WHERE  original_otl = $$SuperToken$$$original_otl$$SuperToken$$ ORDER BY id DESC LIMIT 1;"
    )
    val res = stm_select_cache_id.executeQuery()
    res
  }

  /** Sets lock on Job cache removing because of subsearches. */
  def lockCache(cacheId: Int, lockerId: Int): Unit = {
    val stm_lock_cache = dbConnection.prepareStatement(
      s"INSERT INTO CachesLock (id, locker) VALUES ($cacheId, $lockerId);"
    )
    stm_lock_cache.execute()
  }

  /** Removes lock on Job cache removing. */
  def unlockCache(cacheId: Int, lockerId: Int): Unit = {
    val stm_unlock_cache = dbConnection.prepareStatement(
      s"DELETE FROM CachesLock WHERE id=$cacheId AND locker=$lockerId;"
    )
    stm_unlock_cache.execute()
  }

  /** Removes lock on all Job caches removing. */
  def unlockCaches(lockerId: Int): Unit = {
    val stm_unlock_cache = dbConnection.prepareStatement(
      s"DELETE FROM CachesLock WHERE locker=$lockerId;"
    )
    stm_unlock_cache.execute()
  }

  /** Set all running or new jobs to failed state. */
  def interruptJobs(): Unit = {
    val stm_finalize_jobs = dbConnection.prepareStatement(
      "UPDATE OTLQueries SET status='failed' WHERE status IN ('running', 'new');"
    )
    stm_finalize_jobs.executeUpdate()
  }

  /** Deletes all caches. */
  def clearCaches(): Unit = {
    val stm_clear_caches = dbConnection.prepareStatement(
      "DELETE FROM CachesDL;"
    )
    stm_clear_caches.execute()
  }

  /** Unlocks all caches. */
  def unlockCaches(): Unit = {
    val stm_unlock_caches = dbConnection.prepareStatement(
      "DELETE FROM CachesLock;"
    )
    stm_unlock_caches.execute()
  }

  /** Returns tables from DB. */
  def getDBTables: ResultSet = {
    val username = config.getString("jdbc.username")
    val stm_select_tables = dbConnection.prepareStatement(
      s"SELECT tablename FROM pg_catalog.pg_tables WHERE tableowner='$username' AND schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
    )
    val res = stm_select_tables.executeQuery()
    res
  }

  /** Returns custom types from DB. */
  def getDBTypes: ResultSet = {
    val username = config.getString("jdbc.username")
    val stm_select_types = dbConnection.prepareStatement(
      s"SELECT typname FROM pg_type JOIN pg_user ON pg_user.usesysid = pg_type.typowner WHERE pg_user.usename='$username' AND pg_type.typtype='e';"
    )
    val res = stm_select_types.executeQuery()
    res
  }

  /** Uses for creating statements if DB scheme is empty.
    *
    * @param sql Sting with SQL query.
    */
  def sql_without_results(sql: String): Unit = {
    dbConnection.prepareStatement(sql).execute()
  }

  /** Returns all expired Jobs. */
  def getExpiredQueries: ResultSet = {
    val stm_select_expired_queries = dbConnection.prepareStatement(
      s"SELECT id FROM OTLQueries WHERE status = 'running' AND to_timestamp(extract(epoch from creating_date) + $jobTimeout) < CURRENT_TIMESTAMP;"
    )
    val res = stm_select_expired_queries.executeQuery()
    res
  }

  /** Sets Job state to canceled. */
  def setJobStateCanceled(id: Int): Int = {
    val stm_upd_expired = dbConnection.prepareStatement(s"UPDATE OTLQueries SET status = 'canceled', msg='timeout' WHERE id = $id;")
    stm_upd_expired.executeUpdate()
  }

  def getJobState(id: Int): String = {
    val stm_select_job_state = dbConnection.prepareStatement(s"SELECT status FROM OTLQueries WHERE id = $id;")
    val res = stm_select_job_state.executeQuery()
    var state: String = "notfound"
    if (res.next()) {
      state = res.getString("status")
    }
    state
  }

  def firstTick(applicationId: String): Long = {
    val stm_upd_tick = dbConnection.prepareStatement(s"INSERT INTO Ticks (applicationId) VALUES('$applicationId') RETURNING extract(epoch from lastCheck) as lastCheck;")
    val res = stm_upd_tick.executeQuery()
    res.next()
    val lastCheck = res.getLong("lastCheck")
    lastCheck
  }

  def tick(applicationId: String): Long = {
    val stm_upd_tick = dbConnection.prepareStatement(s"UPDATE Ticks SET lastCheck = now() WHERE applicationId = '$applicationId' RETURNING extract(epoch from lastCheck) as lastCheck;")
    val res = stm_upd_tick.executeQuery()
    res.next()
    val lastCheck = res.getLong("lastCheck")
    lastCheck
  }

}
