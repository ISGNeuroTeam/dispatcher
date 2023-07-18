package ot.dispatcher

import com.isgneuro.sparkexecenv.{BaseCommand, CommandExecutor, CommandsProvider}
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import ot.AppConfig
import ot.AppConfig._
import ot.dispatcher.kafka.context.JobsContainer
import ot.dispatcher.sdk.core.CustomException
import ot.dispatcher.sdk.core.CustomException.E00017
import play.api.libs.json.JsValue

import java.net.URI
import java.sql.ResultSet
import java.util.{Calendar, UUID}
import scala.collection.mutable.ArrayBuffer
import scala.sys.process.Process

/** Gets settings from config file and then runs infinitive loop of user's and system's queries.
 *
 * 1. Loads logger.
 * 2. Generate computing node uuid.
 * 3. Loads Spark's session and update runtime configs.
 * 4. Loads connector to DB.
 * 5. Load interactor with Kafka.
 * 6. Kafka service existing checking.
 * 7. Loads RAM cache manager.
 * 8. Loads Spark checkpoints manager.
 * 9. Loads calculation manager.
 * 10. Loads execution environment commands provider.
 * 11. Register computing node in Kafka.
 * 12. Runs restoring actions after reboot or first start.
 * 13. Runs infinitive loop of system maintenance and user's queries.
 * 14. Unregister computing node in Kafka.
 *
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
class SuperVisor {

  // Step 1. Loads logger.
  val log: Logger = Logger.getLogger("VisorLogger")
  log.setLevel(Level.toLevel(getLogLevel(config, "visor")))
  //Step 2. Generate computing node uuid.
  var computingNodeUuid: String = getComputingNodeUuid().toString.replace("-", "")
  log.info(s"Computing node uuid: ${computingNodeUuid.toString}")
  // Step 3. Loads Spark's session and context and runtime configs.
  val sparkSession: SparkSession = getSparkSession
  val sparkContext: SparkContext = sparkSession.sparkContext
  log.info("SparkSession started.")
  // Step 4. Loads connector to DB.
  val superDbConnector = new SuperDbConnector()
  log.info("SuperDbConnector is ready.")
  //Step 5. Load interactor with Kafka.
  val kafkaIpAddress: String = config.getString("kafka.ip_address")
  val kafkaPort: Int = config.getInt("kafka.port")
  val kafkaExists: Boolean = config.getBoolean("kafka.computing_node_mode_enabled")
  val computingNodeInteractor: ComputingNodeInteractable = if (kafkaExists){new ComputingNodeInteractor(kafkaIpAddress, kafkaPort)} else {new NullComputingNodeInteractor}
  //Step 6. Kafka service existing checking.
  if (kafkaExists) {
    log.info("SuperKafkaConnector is ready.")
    log.info("Computing Node Mode is enabled")
    log.info(s"Kafka ip address: ${kafkaIpAddress}")
    log.info(s"Kafka port: ${kafkaPort}")
  } else {
    log.info("Computing Node Mode is disabled")
  }
  // Step 7. Loads RAM cache manager.
  val cacheManager = new CacheManager(sparkSession)
  log.info("CacheManager started.")

  // Step 8. Loads Spark checkpoints manager.
  val checkpointsManager = new CheckpointsManager(sparkSession)
  log.info("CheckpointsManager started.")

  // Step 9. Loads calculation manager.
  val superCalculator = new SuperCalculator(cacheManager, superDbConnector)
  log.info("SuperCalculator started.")

  //Step 10. Loads execution environment commands provider.
  val commandsProvider: Option[CommandsProvider] = if (kafkaExists) {
    Some(new CommandsProvider(config.getString("usercommands.directory"), log))
  } else {
    None
  }


  //Identifiers of executing jobs (For Dispatcher without and with Kafka )
  var activeJobIds = new ArrayBuffer[Int]()
  var jobIds = new ArrayBuffer[String]()

  /** Starts infinitive loop. */
  def run(): Unit = {
    log.info("SuperVisor started.")
    // Step 11. Register computing node in Kafka.
    if (kafkaExists) {
      //host id defining through Java sys.process
      val p = Process("hostid")
      val hostId: String = p.!!.trim()
      //Node registartion
      computingNodeInteractor.registerNode(computingNodeUuid, hostId, commandsProvider.get)
      log.info(s"Registering Node with ID ${computingNodeUuid}, Host ID: ${hostId}")
      log.info("Spark computing node registered in Kafka")
    }
    // Step 12. Runs restoring actions after reboot or first start.
    restorationMaintenance()
    log.info("Dispatcher restored DB and RAM Cache.")
    // Step 13. Runs infinitive loop of system maintenance and user's queries.
    runInfiniteLoop()
    // Step 14. Unregister computing node in Kafka.
    if (kafkaExists) {
      computingNodeInteractor.unregisterNode(computingNodeUuid)
      log.info(s"Unregistering Node with ID ${computingNodeUuid}")
    }
  }

  /**
   * Get computing node uuid from config or generate it if not exists
   * @return computing node uuid
   */
  def getComputingNodeUuid(): UUID = {
    try {
      UUID.fromString(config.getString("node.uuid"))
    } catch {
      case e: Exception => generateNodeUuid()
    }
  }

  /**
   * Generate computing node uuid
   * @return computing node uuid
   */
  private def generateNodeUuid(): UUID = {
    val bytesContainer: String = "uuid_text"
    UUID.nameUUIDFromBytes(bytesContainer.getBytes())
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

    if (kafkaExists) {
      //Launch process of getting jobs from Kafka in separate thread
      computingNodeInteractor.launchJobsGettingProcess(computingNodeUuid)
    }
    //Importing commands for executing in spark execution environment
    val commandClasses:  Map[String, Class[_ <: BaseCommand]] = if (kafkaExists) {commandsProvider.get.commandClasses} else {Map()}

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
        userMaintenance(commandClasses)
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
      "checkpointsManager" -> checkpointsManager,
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
      "sparkSession" -> sparkSession,
      "nodeInteractor" -> computingNodeInteractor,
      "computingNodeUuid" -> computingNodeUuid,
      "kafkaExists" -> kafkaExists
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
  def userMaintenance(commandClasses: Map[String, Class[_ <: BaseCommand]]): Unit = {
    // Gets new Jobs.
    val res = superDbConnector.getNewQueries
    val commandStructs = JobsContainer.syncValues

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
    if (kafkaExists) {
      try {
        var cmJson = commandStructs.poll().asInstanceOf[JsValue]
        while (cmJson != null) {
          //log.info("cmJson" + cmJson.toString() + cmJson.getClass.getName)
          val jobUuid = (cmJson \ "uuid").as[String]
          val status = (cmJson \ "status").as[String]
          //Case of job cancelling
          if (status == "CANCELLED") {
            if (jobIds.contains(jobUuid)) {
              sparkContext.cancelJobGroup(jobUuid)
              jobIds.remove(jobIds.indexOf(jobUuid))
              log.info(s"Job with uuid ${jobUuid} was canceled.")
            } else {
              log.info(s"Job with uuid ${jobUuid} isn't exists among launched jobs.")
            }
          } //Case of job executing
          else if (status == "READY_TO_EXECUTE") {
            log.info("Ready to exec " + jobUuid)
            val jsCommands = cmJson \ "commands"
            //log.info("Js commands: " + jsCommands.toString + jsCommands.getClass.getName)
            val execEnvFuture = Future(execEnvFutureCalc(jobUuid, jsCommands.as[List[JsValue]], commandClasses))
            execEnvFuture.onComplete {
              case Success(value) =>
                log.info(s"Future Job ${jobUuid} is finished.")
              case Failure(exception) =>
                log.error(s"Future failed: ${exception.getLocalizedMessage}.")
                val jobStatusText = exception.getLocalizedMessage
                computingNodeInteractor.jobStatusNotify(jobUuid, "FAILED", jobStatusText)
                log.error(s"Failed ${jobUuid} ", exception)
            }
          }
          cmJson = commandStructs.poll().asInstanceOf[JsValue]
        }
      } catch {
        case e: Exception => computingNodeInteractor.errorNotify(computingNodeUuid, e.getLocalizedMessage)
          log.error("Error ", e)
      }
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
        // Add id of active job to list
        activeJobIds += otlQuery.id
        // Set job group for timeout support.
        sparkContext.setJobGroup(s"Search ID ${otlQuery.id}", s"Job was requested by ${otlQuery.username}", interruptOnCancel = true)
        // Change pool to pool_user.
        sparkContext.setLocalProperty("spark.scheduler.pool", s"pool_${otlQuery.username}")
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
      } finally {
        synchronized {
          if (activeJobIds.toArray.diff(Array(otlQuery.id)).isEmpty) {
            log.debug("Checkpoints data deleting...")
            sparkContext.getCheckpointDir match {
              case Some(dir) =>
                val fs = org.apache.hadoop.fs.FileSystem.get(new URI(dir), sparkContext.hadoopConfiguration)
                fs.delete(new Path(dir), true)
              case None =>
            }
            log.debug("Checkpoints data deleted.")
          }
          val jobIndex = activeJobIds.indexOf(otlQuery.id)
          activeJobIds.remove(jobIndex)
        }
      }
    }
  }

  /**
   * Process job and send it to execution enviromnent for result generating
   * @param jobUuid unique identifier of job
   * @param otlCommands list of receiving otl commands in JSON format
   * @param commandClasses map of command names to extending BaseCommand OTL class classes-transformers
   */
  def execEnvFutureCalc(jobUuid: String, otlCommands: List[JsValue], commandClasses: Map[String, Class[_ <: BaseCommand]]) = {
    import scala.concurrent.blocking
    blocking {
      sparkContext.setJobGroup(jobUuid, s"jobs of uuid ${jobUuid}")
      jobIds +: jobUuid
      val commandsExecutor = new CommandExecutor(commandClasses, computingNodeInteractor.logProgressMessage)
      commandsExecutor.execute(jobUuid, otlCommands)
      computingNodeInteractor.jobStatusNotify(jobUuid, "FINISHED", s"Job ${jobUuid} was finished succesfully")
    }
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

}