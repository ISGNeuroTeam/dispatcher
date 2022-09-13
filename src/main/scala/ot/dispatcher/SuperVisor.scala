package ot.dispatcher

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import ot.AppConfig
import ot.AppConfig._
import ot.dispatcher.kafka.context.CommandsContainer
import ot.dispatcher.sdk.core.CustomException
import ot.dispatcher.sdk.core.CustomException.E00017

import java.sql.ResultSet
import java.util.{Calendar, UUID}

/** Gets settings from config file and then runs infinitive loop of user's and system's queries.
 *
 * 1. Loads logger.
 * 2. Loads Spark's session and update runtime configs.
 * 3. Loads connector to DB.
 * 4. Loads RAM cache manager.
 * 5. Loads calculation manager.
 * 6. Runs restoring actions after reboot or first start.
 * 7. Runs infinitive loop of system maintenance and user's queries.
 *
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
class SuperVisor {

  // Step 1. Loads logger.
  val log: Logger = Logger.getLogger("VisorLogger")
  log.setLevel(Level.toLevel(getLogLevel(config, "visor")))
  //Generate computing node uuid
  val computingNodeUuid = UUID.randomUUID
  // Step 2. Loads Spark's session and runtime configs.
  val sparkSession: SparkSession = getSparkSession
  log.info("SparkSession started.")
  // Step 3. Loads connector to DB.
  val superDbConnector = new SuperDbConnector()
  log.info("SuperDbConnector is ready.")
  //Step 4. Load connector to Kafka.
  val superKafkaConnector = new SuperKafkaConnector("otl_kafka_stream")
  val kafkaExists: Boolean = config.getString("kafka.computing_node_mode_enabled").asInstanceOf[Boolean]
  if (kafkaExists) {
    log.info("SuperKafkaConnector is ready.")
  } else {
    log.info("SuperKafkaConnector is not ready. Kafka service isn't exists in system.")
  }
  // Step 5. Loads RAM cache manager.
  val cacheManager = new CacheManager(sparkSession)
  log.info("CacheManager started.")
  // Step 5. Loads calculation manager.
  val superCalculator = new SuperCalculator(cacheManager, superDbConnector)
  log.info("SuperCalculator started.")

  /** Starts infinitive loop. */
  def run(): Unit = {

    log.info("SuperVisor started.")
    // Step 6. Runs restoring actions after reboot or first start.
    restorationMaintenance()
    log.info("Dispatcher restored DB and RAM Cache.")
    // Step 7. Register computing node in Kafka
    if (kafkaExists) {
      registerNode()
      log.info("Spark computing node registered in Kafka")
    }
    // Step 8. Runs infinitive loop of system maintenance and user's queries.
    runInfiniteLoop()
    // Step 9. Unregister computing node in Kafka
    if (kafkaExists) {
      unregisterNode()
      log.info("Spark computing node unregistered in Kafka")
    }
  }

  /** Returns Spark session and loads config.
   *
   * @return Spark's session instance.
   */
  def getSparkSession: SparkSession = {

    val spark = SparkSession.builder()
      .appName(config.getString("spark.appName"))
      .master(config.getString("spark.master"))
      .getOrCreate()

    AppConfig.updateConfigWith(spark.conf.getOption("spark.application.config"))
    scala.util.Properties.setProp("files.log_localisation", AppConfig.config.getString("files.log_localisation"))
    log.setLevel(Level.toLevel(getLogLevel(config, "visor")))

    spark.sparkContext.setLogLevel(getLogLevel(config, "spark"))
    spark
  }

  def registerNode() = {
    var hostId = UUID.randomUUID
    while (hostId.compareTo(computingNodeUuid) == 0) {
      hostId = UUID.randomUUID
    }
    val commandName = "REGISTER_COMPUTING_NODE"
    val registerMessage =
      s"""
         |{
         |"computing_node_uuid": "${computingNodeUuid}",
         |"command_name": "${commandName}",
         |"command": {
         |    "computing_node_type": "SPARK",
         |    "host_id": "${hostId}",
         |    "resources": {
         |      "job_capacity": 999999999
         |    }
         |  }
         |}
         |""".stripMargin
    superKafkaConnector.sendMessage("computing_node_control", commandName, registerMessage)
    println("id: " + computingNodeUuid.toString)
  }

  /** Starts infinitive loop with System's and User's maintenance.
   * System's one consists of caches and buckets managing.
   * User's one consists of search queries.
   */
  def runInfiniteLoop(): Unit = {
    log.debug("Infinite Loop started.")
    val pause = config.getInt("loop.pause")
    var loopEndTime = Calendar.getInstance().getTimeInMillis

    var negativeDeltaCounter: Int = 0
    val negativeWarnThreshold: Int = config.getInt("loop.negative_warn_threshold")

    new Thread(){
      override def run(): Unit = superKafkaConnector.getNewCommands(computingNodeUuid.toString)}.start()

    while (true) {
      val delta = Calendar.getInstance().getTimeInMillis - loopEndTime

      if (delta < 0) {
        negativeDeltaCounter += 1
        log.info(s"Delta has a negative value: $delta. Work will be continued.")

        if (negativeDeltaCounter >= negativeWarnThreshold)
          log.warn(s"WARNING delta value has been negative for the last $negativeWarnThreshold times. Please inform administrators.")

        loopEndTime = Calendar.getInstance().getTimeInMillis
      } else if (delta > pause) {
        negativeDeltaCounter = 0

        systemMaintenance()
        userMaintenance()
        loopEndTime = Calendar.getInstance().getTimeInMillis
      } else {
        Thread.sleep(delta)
      }
    }
  }

  def restorationMaintenance(): Unit = {
    log.trace("Restoration Maintenance section started.")
    val restorationMaintenanceArgs = Map(
      "superConnector" -> superDbConnector,
      "cacheManager" -> cacheManager,
      "sparkSession" -> sparkSession
    )

    val rm = new RestorationMaintenance(restorationMaintenanceArgs)
    rm.run()

  }

  /** Runs System's maintenance.
   * For simple development time uses Map with all needed submodules as args for [[SystemMaintenance]] where you also
   * can find list of jobs.
   */
  def systemMaintenance(): Unit = {
    // TODO Remove initialization of sm and sM in each loop.
    log.trace("System Maintenance section started.")
    val systemMaintenanceArgs = Map(
      "cacheManager" -> cacheManager,
      "superConnector" -> superDbConnector,
      "sparkSession" -> sparkSession
    )
    val sm = new SystemMaintenance(systemMaintenanceArgs)
    sm.run()
    log.trace("System Maintenance section finished.")
  }

  /** Runs User's maintenance.
   * 1. Gets all new jobs from DB.
   * 2. Marks Job as running.
   * 3. Starts future with it's calculation.
   * 4. Depending on state marks it failed or finished.
   */
  def userMaintenance(): Unit = {
    // Gets new Jobs.
    val res = superDbConnector.getNewQueries
    val commandStructs = CommandsContainer.syncValues

    import scala.concurrent.{ExecutionContext, Future}
    import ExecutionContext.Implicits.global
    import scala.util.{Failure, Success}
    // Starts for each Job calculation process in Future.
    while (res.next()) {

      val otlQuery = getOTLQueryObject(res)
      log.info(otlQuery)
      log.debug(s"Job ${otlQuery.id} is setting to running.")
      superDbConnector.setJobStateRunning(otlQuery.id)
      val future = Future(futureCalc(otlQuery))
      // Sets logging for future branches depending on it's final state.
      future.onComplete {
        case Success(id) => log.info(s"Future Job $id is finished.")
        case Failure(error) =>
          log.error(s"Future failed: ${error.getLocalizedMessage}.")
          error.printStackTrace()
      }
      log.info(s"Job ${otlQuery.id} is running")
    }
  }

  /** Returns Job ID for logging.
   * Makes calculation, saves cache and works with exceptions.
   *
   * @param otlQuery Job object from DB.
   * @return Job ID.
   */
  def futureCalc(otlQuery: OTLQuery): Integer = {

    import scala.concurrent.blocking
    blocking {

      try {
        // Set job group for timeout support.
        sparkSession.sparkContext.setJobGroup(s"Search ID ${otlQuery.id}", s"Job was requested by ${otlQuery.username}", interruptOnCancel = true)
        // Change pool to pool_user.
        sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", s"pool_${otlQuery.username}")
        // Starts main calculation process.
        superCalculator.calc(otlQuery)
        if (otlQuery.cache_ttl != 0) {
          // Registers new cache id DB.
          superDbConnector.addNewCache(otlQuery)
        }
        // Marks Job in DB as finished.
        superDbConnector.setJobStateFinished(otlQuery.id)
        otlQuery.id

      } catch {

        // Error branch. Marks Job as failed.
        case error: CustomException =>
          val jobState = superDbConnector.getJobState(otlQuery.id)
          if (jobState == "canceled") {
            throw E00017(otlQuery.id)
          } else {
            superDbConnector.setJobStateFailed(otlQuery.id, error.getLocalizedMessage)
            superDbConnector.unlockCaches(otlQuery.id)
            throw error
          }
        case error: OutOfMemoryError =>
          superDbConnector.setJobStateFailed(otlQuery.id, error.getLocalizedMessage)
          superDbConnector.unlockCaches(otlQuery.id)
          throw error
        case error: Exception =>
          superDbConnector.setJobStateFailed(otlQuery.id, error.getLocalizedMessage)
          superDbConnector.unlockCaches(otlQuery.id)
          throw error
        case throwable: Throwable =>
          superDbConnector.setJobStateFailed(otlQuery.id, throwable.getLocalizedMessage)
          superDbConnector.unlockCaches(otlQuery.id)
          throw throwable
      }
    }
  }

  def notifyError(error: String): Unit = {
    val commandName = "ERROR_OCCURED"
    val errorMessage =
      s"""
         |{
         |"computing_node_uuid": "${computingNodeUuid}",
         |"command_name": "${commandName}"
         |"command": {
         |    "error": "${error}"
         |  }
         |}
         |""".stripMargin
    superKafkaConnector.sendMessage("computing_node_control", commandName, errorMessage)
  }

  def sha256Hash(text: String): String = String.format(
    "%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256")
      .digest(text.trim.getBytes("UTF-8"))
    )
  )

  /** Returns instance of [[ot.dispatcher.OTLQuery]] case class from parsed SQL query.
   * Parses DB response to Scala types.
   *
   * @param res [[org.postgresql.jdbc.PgResultSet]] from SQL query to DB.
   * @return instance of case class with all needed information about Job.
   */
  def getOTLQueryObject(res: ResultSet): OTLQuery = {
    val subsearches_array = if (res.getArray("subsearches") != null) res.getArray("subsearches").getArray.asInstanceOf[Array[String]] else Array[String]()
    var subsearches_map = Map[String, String]()
    if (subsearches_array.nonEmpty) {
      subsearches_array.foreach(subsearch => {
        subsearches_map = subsearches_map + (s"subsearch_${sha256Hash(subsearch)}" -> subsearch)
      })
    }
    val otlQuery = OTLQuery(
      id = res.getInt("id"),
      original_otl = res.getString("original_otl"),
      service_otl = res.getString("service_otl"),
      tws = res.getInt("tws"),
      twf = res.getInt("twf"),
      cache_ttl = res.getInt("cache_ttl"),
      // Scala brain damage magic. If you know how to get Array in less complicated way please tell me.
      indexes = if (res.getArray("indexes") != null) res.getArray("indexes").getArray.asInstanceOf[Array[String]] else Array[String](),
      subsearches = subsearches_map,
      username = res.getString("username"),
      field_extraction = res.getBoolean("field_extraction"),
      preview = res.getBoolean("preview")
    )
    otlQuery
  }

  def unregisterNode() = {
    val commandName = "UNREGISTER_COMPUTING_NODE"
    val unregisterMessage =
      s"""
         |{
         |"computing_node_uuid": "${computingNodeUuid}",
         |"command_name": "${commandName}",
         |"command": {}
         |}
         |""".stripMargin
    superKafkaConnector.sendMessage("computing_node_control", commandName, unregisterMessage)
  }

}
