package ot.scalaotl
package commands

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import ot.AppConfig.{config, getLogLevel}
import ot.dispatcher.sdk.core.CustomException.{E00012, E00013, E00014}
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers._
import ot.scalaotl.utils.logging.StatViewer

import scala.util.{Failure, Success, Try}


/** =Abstract=
 * This class is the base class for all OTL-command classes
 *
 * @param sq [[SimpleQuery]] - contains args, searchId, cache, subsearches, search time interval, stfe and preview flags
 * sq comes from the OTLQuery and Converter classes
 * @param _seps [[ Set[String] ]] - used in some OTL-commands (like OTLchart, OTLDedup, OTLLookup and many others),
 * _seps empty by default, but can be overridden in the constructor of the derived class
 */
abstract class OTLBaseCommand(sq: SimpleQuery, _seps: Set[String] = Set.empty) extends OTLSparkSession with DefaultParser {
  val args: String = sq.args
  val seps: Set[String] = _seps

  val keywords: List[Keyword] = keywordsParser(args)
  val keywordsMap: Map[String, Field] = fieldsToMap(keywords)
  val positionals: Seq[Positional] = positionalsParser(args, seps)
  val positionalsMap: Map[String, Field] = fieldsToMap(positionals)
  val returns: Return = returnsParser(args, seps)
  // may be overrided in derived class
  val requiredKeywords: Set[String]
  // may be overrided in derived class
  val optionalKeywords: Set[String]

  def fieldsUsed: List[String] = getFieldsUsed(returns)

  def fieldsGenerated: List[String] = getFieldsGenerated(returns)

  var fieldsUsedInFullQuery: Seq[String] = Seq[String]()

  def setFieldsUsedInFullQuery(fs: Seq[String]): OTLBaseCommand = {
    fieldsUsedInFullQuery = fs
    this
  }

  val className: String = this.getClass.getSimpleName

  def commandName: String = this.getClass.getSimpleName.toLowerCase.replace("otl", "")

  def loggerName: String = this.getClass.getName

  def log: Logger = Logger.getLogger(loggerName)

  def logLevel: String = getLogLevel(config, className)

  log.setLevel(Level.toLevel(logLevel))

  def getKeyword(label: String): Option[String] = {
    keywordsMap.get(label).map {
      case Keyword(k, v) => v
    }
  }

  def getPositional(label: String): Option[List[String]] = {
    positionalsMap.get(label).map {
      case Positional(k, v) => v
    }
  }

  /**
   * Validates command args. Called from safeTransform method
   */
  def validateArgs(): Unit = {
    validateRequiredKeywords()
    // validateOptionalKeywords()
  }

  /**
   * Checker for required keywords.
   * Compares found keywords and required keywords.
   * Throws an exception if any of the required arguments is not among the command arguments
   * May be overrided in derived class
  */
  def validateRequiredKeywords(): Unit = {
    val notFoundArgs = requiredKeywords.diff(keywordsMap.keys.toSet)
    if (notFoundArgs.nonEmpty) {
      val notFoundString = notFoundArgs.mkString("['", "', '", "']")
      throw E00012(sq.searchId, commandName, notFoundString)
    }
  }

  /**
   * Checker for optional keywords.
   * Compares found keywords and required keywords.
   * Throws an exception if any of the arguments is not among the required or optional arguments
   * May be overrided in derived class
   */
  def validateOptionalKeywords(): Unit = {
    val unknownKeywords = keywordsMap.keys.toSet.diff(requiredKeywords.union(optionalKeywords))
    if (unknownKeywords.nonEmpty) {
      val unknownKeysString = unknownKeywords.mkString("['", "', '", "']")
      throw E00013(sq.searchId, commandName, unknownKeysString)
    }
  }

  /**
   * Wrapper with logging for the transform method. Called by Converter for each OTL-command.
   * Step1. Logs all command params.
   * Step2. Logs fieldsUsed, fieldsGenerated, input dataframe schema.
   * Step3. Calls transfrom method of OTL-command.
   * Step4. Logs results of OTL-command
   *
   * @param _df [[DataFrame]] - incoming dataset (in generator-command like this one is ignored and should be empty)
   * @return [[DataFrame]]  - outgoing dataframe with OTL-command results
   */
  def loggedTransform(_df: DataFrame): DataFrame = {
    log.debug(f"[SearchId:${sq.searchId}] ======= Starting command $commandName ========")
    log.debug(f"[SearchId:${sq.searchId}]Starting query with args: ${sq.args}," +
      f" id: ${sq.searchId}, " +
      f"cache: ${sq.cache.keySet.mkString("[", ", ", "]")}," +
      f" subsearches: ${sq.subsearches.map(s => s._1 + ":" + s._2).mkString("[", ", ", "]")}," +
      f" startTime: ${sq.tws}," +
      f" finishTime: ${sq.twf}," +
      f" searchTimeFieldExtractionEnables: ${sq.searchTimeFieldExtractionEnables}")
    log.debug(f"[SearchId:${sq.searchId}]Fields used in query: ${fieldsUsed.mkString("[", ", ", "]")}")
    log.debug(f"[SearchId:${sq.searchId}]Fields will be generated: ${fieldsGenerated.mkString("[", ", ", "]")}")
    log.debug(f"[SearchId:${sq.searchId}]Fields in input dataframe schema ${
      _df.schema.fields.map(f => f.dataType + ":" + f.name).mkString("[", ", ", "]")
    }")
    val res = transform(_df)
    log.debug(f"[SearchId:${sq.searchId}]Parsing attribute returns : ${returns.flatFields.mkString("[<", ">, <", ">]")}")
    log.debug(f"[SearchId:${sq.searchId}]Parsing attribute keywords : ${keywords.mkString("[", ", ", "]")}")
    log.debug(f"[SearchId:${sq.searchId}]Parsing attribute positionals : ${positionals.mkString("[", ", ", "]")}")
    log.debug(f"[SearchId:${sq.searchId}]Fields in output dataframe schema ${
      res.schema.fields
        .map(f => f.dataType + ":" + f.name)
        .mkString("[", ", ", "]")
    }")
    if (log.getLevel == Level.DEBUG) {
      log.debug(f"[SearchId:${sq.searchId}]\n" + StatViewer.getPreviewString(res))
    }
    log.debug(f"[SearchId:${sq.searchId}]======= End command $commandName ========")
    res
  }

  /**
   * Method that implements the main logic of the OTL-command. Must be overridden in the derived class
   *
   * @param _df [[DataFrame]] - incoming dataset (in generator-command like this one is ignored and should be empty)
   * @return [[DataFrame]]  - outgoing dataframe with OTL-command results
   */
  def transform(_df: DataFrame): DataFrame

  // not used
  def getTransform: DataFrame => DataFrame = transform

  /**
   * Wrapper for the logged transform method that handles all exceptions
   * Called by Converter for each OTL-command. Calls loggedTransform method
   *
   * @param _df [[DataFrame]] - incoming dataset (in generator-command like this one is ignored and should be empty)
   * @return [[DataFrame]]  - outgoing dataframe with OTL-command results
   */
  def safeTransform(_df: DataFrame): DataFrame = {
    import java.lang.reflect.InvocationTargetException
    validateArgs()
    val nullFields = fieldsUsed.distinct.map(_.stripBackticks()).diff(_df.columns.map(_.stripBackticks())).filter(!_.contains("*"))
    val ndf = nullFields.foldLeft(_df) { (acc, col) => acc.withColumn(col, lit(null)) }
    Try(loggedTransform(ndf)) match {
      case Success(df) => df
      case Failure(ex) if ex.getClass.getSimpleName.contains("CustomException") =>
        log.error(ex.getMessage)
        throw ex
      //Working with udf exceptions when call df.show
      case Failure(ex: InvocationTargetException) =>
        val exception = ex.getTargetException match {
          case e1 if !e1.getMessage.startsWith("Job aborted due to stage failure") => e1
          case e1 => e1.getCause match {
            case e2 if !e2.getMessage.startsWith("Failed to execute user defined function") => e2
            case e2 => e2.getCause
          }
        }
        log.error(f"Error in  '$commandName' command: ${exception.getMessage}")
        throw E00014(sq.searchId, commandName, exception)
      case Failure(ex) =>
        log.error(f"Error in  '$commandName' command: ${ex.getMessage}")
        throw E00014(sq.searchId, commandName, ex)
    }
  }
}
