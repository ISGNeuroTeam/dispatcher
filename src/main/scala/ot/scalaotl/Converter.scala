package ot.scalaotl

import com.typesafe.config.ConfigException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import ot.AppConfig
import ot.AppConfig._
import ot.dispatcher.OTLQuery
import ot.dispatcher.sdk.core.CustomException.E00002
import ot.scalaotl.commands._
import ot.scalaotl.commands.service.ReloadCommand
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.StringExt._

/**
 * Transforms OTL queries to Spark queries, calculate resulting Spark DataFrame.
 *
 * @constructor create a new converter for particular OTL query
 * @param otlQuery [[OTLQuery]] - OTL query
 * @param cache    [[Map[String, DataFrame]]] - cache for previously calculated dataframes
 * @author Nikolay Ryabykh (nryabykh@ot.ru)
 */

class Converter(otlQuery: OTLQuery, cache: Map[String, DataFrame]) extends OTLSparkSession {

  /** Simple constructor for queries without cache */
  def this(otlQuery: OTLQuery) = {
    this(otlQuery, Map[String, DataFrame]())
  }

  val classname = this.getClass.getSimpleName
  val log: Logger = Logger.getLogger(this.getClass.getName)
  log.setLevel(Level.toLevel(getLogLevel(ot.AppConfig.config, classname)))


  log.debug(f"Procesing Query with id: ${otlQuery.id}," +
    f" original_otl: ${otlQuery.original_otl}," +
    f" service_otl: ${otlQuery.service_otl}," +
    f" tws: ${otlQuery.tws}," +
    f" twf: ${otlQuery.twf}," +
    f" cache_ttl: ${otlQuery.cache_ttl}, " +
    f"indexes: ${otlQuery.indexes.mkString("[", ", ","]")}," +
    f" subsearches: ${otlQuery.subsearches.map(s => s._1 + ":" + s._2).mkString("[", ", ","]")}," +
    f" username: ${otlQuery.username}," +
    f" field_extraction: ${otlQuery.field_extraction}");

  var df: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq(StructField("_raw", StringType))))
  val query: String = otlQuery.service_otl
    .withKeepQuotedText(str => str.replaceAll("""```(.|\n)*```""",""), SINGLE, DOUBLE)

  // Full query splitted by separate commands
  val commands = query.withKeepQuotedText[List[String]](splitQuery)

  // Instances of classes for perfoming dataframe transformations
  val transformers = getTransformers(commands)

  // List of fields used in full query
  val fieldsUsed = getFieldsUsedInQuery(transformers)
  log.debug(f"Fields used in full query ${fieldsUsed.mkString("[", ", ","]")}")

  def splitQuery(s: String): List[String] = {
    val preResult: List[String] = s.withKeepQuotedText[String](str => str.replace("\\n", ""))
    .withKeepTextInBrackets(_.split("\\|").toSeq,"\\[","\\]")
    .map(_.trim)
    .filterNot(_.isEmpty)
    .toList
    val hasCheckpoints: Boolean = preResult.exists(_.contains("checkpoints"))
    if (hasCheckpoints) {
      if (!preResult.last.contains("checkpoints") || preResult.count(_.contains("checkpoints")) > 1) {
        throw E00002(otlQuery.id, "Command checkpoints should be only one in query and last in query's commands list")
      }
      val checkpointsCommand = preResult.last
      checkpointsCommand :: preResult.reverse.tail.reverse
    } else {
      AppConfig.withCheckpoints = AppConfig.config.getString("checkpoints.enabled") match {
        case "true" => true
        case _ => false
      }
      preResult
    }
  }

  def getTransformers(commands: Seq[String]): Seq[OTLBaseCommand] = {
    val commandPattern = """^(\w+)\s*(.*)""".r
    commands.map { x =>
    {
      val commandPattern(cmd, args) = x
      val sq = new SimpleQuery(
        args = collectSubsearch(cmd, args),
        searchId = otlQuery.id,
        cache = cache,
        subsearches = otlQuery.subsearches,
        tws = otlQuery.tws,
        twf = otlQuery.twf,
        searchTimeFieldExtractionEnables = otlQuery.field_extraction,
        preview = otlQuery.preview
      )
      Converter.getClassByName(cmd, sq)
    }
    }
  }

  def getFieldsUsedInQuery(transformers: Seq[OTLBaseCommand]): Seq[String] = {
    val totalFieldsUsed = transformers.flatMap {
      tr => tr.fieldsUsed.map(_.stripBackticks())
    }
    // val totalFieldsGenerated = transformers.map(_.fieldsGenerated.map(_.strip("`"))).flatten
    totalFieldsUsed.distinct
  }

  def run: DataFrame = {
    log.debug("Running of converter started.")
    var counter = 0
    var checkpointsLimitUsed: String = ""
    if (transformers.exists(_.getClass.getSimpleName == "OTLCheckpoints")) {
      checkpointsLimitUsed = transformers.head.asInstanceOf[OTLCheckpoints].usedLimit
    }
    var checkpointsCommandsLimitUsed = if (checkpointsLimitUsed == "commands")
        true
      else if (checkpointsLimitUsed == "plan_size")
        false
      else
        AppConfig.config.getBoolean("checkpoints.use_commands_limit")
    var commandsLimit = 1
    try {
      commandsLimit = AppConfig.config.getInt("checkpoints.commands_limit")
    } catch {
      case ex: ConfigException.Missing => checkpointsCommandsLimitUsed = false
    }
    var checkpointsPlanSizeLimitUsed = if (checkpointsLimitUsed == "plan_size")
        true
      else if (checkpointsLimitUsed == "commands")
        false
      else
        AppConfig.config.getBoolean("checkpoints.use_plan_size_limit")
    var planSizeLimit = 0
    try {
      planSizeLimit = AppConfig.config.getInt("checkpoints.plan_size_limit")
    } catch {
      case ex: ConfigException.Missing => checkpointsPlanSizeLimitUsed = false
    }
    transformers.foldLeft(df) {
      (accum, tr) =>
      {
        if (tr.getClass.getName.contains("OTLRead") || tr.getClass.getName.contains("OTLInputlookup") || tr.getClass.getName.contains("OTLLookup") || tr.getClass.getName.contains("RawRead") || tr.getClass.getName.contains("FullRead")) tr.setFieldsUsedInFullQuery(fieldsUsed)
        log.debug(s"Cycling item in converter: transformation of ${tr.getClass.getSimpleName} started.")
        val dfTransformed = tr.safeTransform(accum)
        counter += 1
        if (AppConfig.withCheckpoints && ((checkpointsCommandsLimitUsed && counter % commandsLimit == 0)
          || (checkpointsPlanSizeLimitUsed && dfTransformed.queryExecution.logical.toString().length > planSizeLimit))) {

          log.debug(s"Limit by plan size in query reached: checkpointing applied.")
          dfTransformed.checkpoint()

        } else
          dfTransformed
      }
    }
  }

  def findSubsearches(s: String) = {
    val subsearchPattern = """subsearch=(\S+)(\s|$)""".r
    subsearchPattern.findAllIn(s).matchData.map(x => x.group(1)).toList
  }

  def collectSubsearch(cmd: String, args: String): String = {
    val restlikeCommands = List("rest", "otrest", "otrestdo", "otloadjob")
    val joinlikeCommands = List("join", "append", "appendcols", "union")
    val branchCommands = List("foreach", "appendpipe")
    val searchCommands = List("read", "filter")
    val excludeCommands = restlikeCommands ::: joinlikeCommands ::: branchCommands ::: searchCommands
    val subsearches = args.withKeepQuotedText[List[String]](findSubsearches)
    if (subsearches.nonEmpty && !excludeCommands.contains(cmd)) {
      subsearches.foldLeft(args)(
        (accum, ssid) => cache.get(ssid) match {
          case Some(ssdf) => accum.replaceAllLiterally(s"subsearch=$ssid", ssdf.collectToBooleanExpr)
          case _          => accum.replaceAllLiterally(s"subsearch=$ssid", "")
        }
      )
    } else args
  }

  /**
   * Set dataframe for conversion.
   * Use it when you need partial conversion for existing dataframe.
   * Mostly OTL query starts with generating command, so no need to set df manually.
   */
  def setDF(_df: DataFrame): Converter = {
    df = _df
    this
  }
}

/** Object with static methods */
object Converter {

  /**
   * Returns new object for dataframe transformation under particular OTL command
   * Simplified method for commands with no subsearches and other stuff, only with statis arguments
   *
   * @param className [[String]] - command name
   * @param args [[String]] - command args
   * @return instance of corresponding class inherited from [[OTLBaseCommand]]
   */
  def getClassByName(className: String, args: String): OTLBaseCommand = getClassByName(className, SimpleQuery(args))

  /**
   * Returns new object for dataframe transformation under particular OTL command
   *
   * @param className [[String]] - command name
   * @param sq [[SimpleQuery]] - instance with args, cache, subsearches, search time interval
   * @return instance of corresponding class inherited from [[OTLBaseCommand]]
   */
  def getClassByName(className: String, sq: SimpleQuery): OTLBaseCommand = {
    val SimpleQuery(args, searchId, cache, subsearches, tws, twf, stwf, preview) = sq

    className match {
      case "reload" => CommandFactory.loadCommandsInfo(sq.searchId)
        new ReloadCommand(sq)

      case c => CommandFactory.getCommand(c, sq)
    }
  }
}

