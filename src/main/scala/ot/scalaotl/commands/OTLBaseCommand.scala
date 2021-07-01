package ot.scalaotl
package commands

import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers._
import ot.AppConfig.config
import ot.AppConfig.getLogLevel

import scala.util.{Failure, Success, Try}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{NullType, StructField}
import ot.scalaotl.utils.logging.StatViewer
import ot.dispatcher.sdk.core.CustomException.{E00014, E00001, E00013, E00012}

abstract class OTLBaseCommand(sq: SimpleQuery, _seps: Set[String] = Set.empty) extends OTLSparkSession with DefaultParser {
  val args = sq.args
  val seps = _seps

  val keywords = keywordsParser(args)
  val keywordsMap = fieldsToMap(keywords)
  val positionals = positionalsParser(args, seps)
  val positionalsMap = fieldsToMap(positionals)
  val returns = returnsParser(args, seps)
  val requiredKeywords: Set[String]
  val optionalKeywords: Set[String]

  def fieldsUsed = getFieldsUsed(returns)
  def fieldsGenerated = getFieldsGenerated(returns)

  var fieldsUsedInFullQuery = Seq[String]()

  def setFieldsUsedInFullQuery(fs: Seq[String]) = {
    fieldsUsedInFullQuery = fs
    this
  }

  val classname = this.getClass.getSimpleName
  def commandname = this.getClass.getSimpleName.toLowerCase.replace("otl","")

  def loggerName = this.getClass.getName
  def log: Logger = Logger.getLogger(loggerName)
    def logLevel = getLogLevel(config, classname)
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

  def validateArgs(): Unit ={
    validateRequiredKeywords()
    //validateOptionalKeywords()
  }

  def validateRequiredKeywords(): Unit ={
    //if (!requiredArguments.subsetOf(keywordsMap.keys.toSet)){
    val notFoundArgs = requiredKeywords.diff(keywordsMap.keys.toSet)
    if(notFoundArgs.nonEmpty) {
      val notFoundString = notFoundArgs.mkString("['", "', '", "']")
      throw E00012(sq.searchId, commandname, notFoundString)
    }
  }

  def validateOptionalKeywords(): Unit ={
    //    if (!keywordsMap.keys.toSet.subsetOf((requiredArguments.union(optionalArguments))))
    val unknownKeywords = keywordsMap.keys.toSet.diff((requiredKeywords.union(optionalKeywords)))
    if(unknownKeywords.nonEmpty) {
      val unknownKeysString = unknownKeywords.mkString("['", "', '", "']")
      throw E00013(sq.searchId, commandname, unknownKeysString)
    }
  }


  def loggedTransform(_df: DataFrame): DataFrame = {
    log.debug(f"[SearchId:${sq.searchId}] ======= Starting command $commandname ========")
    log.debug(f"[SearchId:${sq.searchId}]Starting query with args : ${sq.args}," +
      f" id: ${sq.searchId}, " +
      f"cache: ${sq.cache.keySet.mkString("[", ", ","]")}," +
      f" subsearches: ${sq.subsearches.map(s => s._1 + ":" + s._2).mkString("[", ", ","]")}," +
      f" startTime: ${sq.tws}," +
      f" finishTime: ${sq.twf}," +
      f" searchTimeFieldExtractionEnables: ${sq.searchTimeFieldExtractionEnables}")
    log.debug(f"[SearchId:${sq.searchId}]Fields used in query: ${fieldsUsed.mkString("[", ", ","]")}")
    log.debug(f"[SearchId:${sq.searchId}]Fields will be generated: ${fieldsGenerated.mkString("[", ", ","]")}")
    log.debug(f"[SearchId:${sq.searchId}]Fields in input dataframe schema ${_df.schema.fields
      .map(f =>f.dataType +":" + f.name)
      .mkString("[", ", ","]")}")
    val res = transform(_df)
    log.debug(f"[SearchId:${sq.searchId}]Parsing attribute returns : ${returns.flatFields.mkString("[<", ">, <",">]")}")
    log.debug(f"[SearchId:${sq.searchId}]Parsing attribute keywords : ${keywords.mkString("[", ", ","]")}")
    log.debug(f"[SearchId:${sq.searchId}]Parsing attribute positionals : ${positionals.mkString("[", ", ","]")}")
    log.debug(f"[SearchId:${sq.searchId}]Fields in output dataframe schema ${res.schema.fields
      .map(f =>f.dataType +":" + f.name)
      .mkString("[", ", ","]")}")
    if (log.getLevel == Level.DEBUG) {
      log.debug(f"[SearchId:${sq.searchId}]\n" + StatViewer.getPreviewString(res))
    }
    log.debug(f"[SearchId:${sq.searchId}]======= End command $commandname ========")
    res
  }

  def transform(_df: DataFrame): DataFrame

  def getTransform: DataFrame => DataFrame = transform

  def safeTransform(_df: DataFrame): DataFrame = {
    import java.lang.reflect.InvocationTargetException
    validateArgs()
    val nullFields = fieldsUsed.distinct.map(_.stripBackticks()).diff(_df.columns.map(_.stripBackticks())).filter(!_.contains("*"))
    val ndf = nullFields.foldLeft(_df) { (acc, col) => acc.withColumn(col, lit(null)) }
    Try(loggedTransform(ndf)) match {
      case Success(df) => df
      case Failure(ex) if (ex.getClass.getSimpleName.contains("CustomException")) => {
        log.error(ex.getMessage)
        throw ex
      }
      //Working with udf exceptions when call df.show
      case Failure(ex : InvocationTargetException) => {
        val exception = ex.getTargetException match {
          case e1 if ! e1.getMessage.startsWith("Job aborted due to stage failure") =>  e1
          case e1 => e1.getCause match {
            case e2 if ! e2.getMessage.startsWith("Failed to execute user defined function") =>  e2
            case e2 => e2.getCause
          }
        }
        log.error(f"Error in  '$commandname' command: ${exception.getMessage}" )
        throw E00014(sq.searchId, commandname, exception)
      }
      case Failure(ex) => {
        log.error(f"Error in  '$commandname' command: ${ex.getMessage}" )
        throw E00014(sq.searchId, commandname, ex)
      }
    }
  }
}
