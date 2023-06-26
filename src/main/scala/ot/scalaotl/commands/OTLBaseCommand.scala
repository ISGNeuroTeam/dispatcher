package ot.scalaotl
package commands

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import ot.AppConfig.{config, getLogLevel}
import ot.dispatcher.sdk.core.CustomException.{E00012, E00013, E00014}
import ot.dispatcher.sdk.proxy.PluginProxyCommand
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers._

import scala.util.{Failure, Success, Try}

abstract class OTLBaseCommand(sq: SimpleQuery, _seps: Set[String] = Set.empty) extends OTLSparkSession with DefaultParser {
  val args: String = sq.args
  val seps: Set[String] = _seps

  val keywords: List[Keyword] = keywordsParser(args)
  val keywordsMap: Map[String, Field] = fieldsToMap(keywords)
  val positionals: Seq[Positional] = positionalsParser(args, seps)
  val positionalsMap: Map[String, Field] = fieldsToMap(positionals)
  val returns: Return = returnsParser(args, seps)
  val requiredKeywords: Set[String]
  val optionalKeywords: Set[String]

  def fieldsUsed: List[String] = (getFieldsUsed(returns) ++ (positionalsMap.values.toList.flatMap(f => f.asInstanceOf[Positional].values))).distinct

  def fieldsGenerated: List[String] = getFieldsGenerated(returns)

  var fieldsUsedInFullQuery: Seq[String] = Seq[String]()

  def setFieldsUsedInFullQuery(fs: Seq[String]): OTLBaseCommand = {
    fieldsUsedInFullQuery = fs
    this
  }

  val classname: String = this.getClass.getSimpleName

  val readingCommandsNames: List[String] = List("OTLInputlookup", "RawRead", "FullRead", "OTLRead")

  def commandNotReading: Boolean = !readingCommandsNames.contains(classname)

  val pluginReadingCommandNames: List[String] = List("ReadFile", "SQLRead", "FSGet")

  def commandNotPluginReading: Boolean = !(classname == "PluginProxyCommand" && pluginReadingCommandNames.contains(this.asInstanceOf[PluginProxyCommand].commandname))

  def commandname: String = this.getClass.getSimpleName.toLowerCase.replace("otl", "")

  def loggerName: String = this.getClass.getName

  def log: Logger = Logger.getLogger(loggerName)

  def logLevel: String = getLogLevel(config, classname)

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

  def validateArgs(): Unit = {
    validateRequiredKeywords()
    //validateOptionalKeywords()
  }

  def validateRequiredKeywords(): Unit = {
    val notFoundArgs = requiredKeywords.diff(keywordsMap.keys.toSet)
    if (notFoundArgs.nonEmpty) {
      val notFoundString = notFoundArgs.mkString("['", "', '", "']")
      throw E00012(sq.searchId, commandname, notFoundString)
    }
  }

  def validateOptionalKeywords(): Unit = {
    val unknownKeywords = keywordsMap.keys.toSet.diff(requiredKeywords.union(optionalKeywords))
    if (unknownKeywords.nonEmpty) {
      val unknownKeysString = unknownKeywords.mkString("['", "', '", "']")
      throw E00013(sq.searchId, commandname, unknownKeysString)
    }
  }

  def loggedTransform(_df: DataFrame): DataFrame = {
    log.debug(f"[SearchId:${sq.searchId}] ======= Starting command $commandname ========")
    log.debug(f"[SearchId:${sq.searchId}]Starting query with args : ${sq.args}," +
      f" id: ${sq.searchId}, " +
      f"cache: ${sq.cache.keySet.mkString("[", ", ", "]")}," +
      f" subsearches: ${sq.subsearches.map(s => s._1 + ":" + s._2).mkString("[", ", ", "]")}," +
      f" startTime: ${sq.tws}," +
      f" finishTime: ${sq.twf}," +
      f" searchTimeFieldExtractionEnables: ${sq.searchTimeFieldExtractionEnables}")
    log.debug(f"[SearchId:${sq.searchId}]Fields used in query: ${fieldsUsed.mkString("[", ", ", "]")}")
    log.debug(f"[SearchId:${sq.searchId}]Fields will be generated: ${fieldsGenerated.mkString("[", ", ", "]")}")
    /*log.debug(f"[SearchId:${sq.searchId}]Fields in input dataframe schema ${
      _df.schema.fields
        .map(f => f.dataType + ":" + f.name)
        .mkString("[", ", ", "]")
    }")*/
    log.debug(s"${this.getClass.getSimpleName}: transform function started")
    val res = transform(_df)
    log.debug(s"${this.getClass.getSimpleName}: transform function finished")
    log.debug(f"[SearchId:${sq.searchId}]Parsing attribute returns : ${returns.flatFields.mkString("[<", ">, <", ">]")}")
    log.debug(f"[SearchId:${sq.searchId}]Parsing attribute keywords : ${keywords.mkString("[", ", ", "]")}")
    log.debug(f"[SearchId:${sq.searchId}]Parsing attribute positionals : ${positionals.mkString("[", ", ", "]")}")
    /*log.debug(f"[SearchId:${sq.searchId}]Fields in output dataframe schema ${
      res.schema.fields
        .map(f => f.dataType + ":" + f.name)
        .mkString("[", ", ", "]")
    }")
    if (log.getLevel == Level.DEBUG) {
      log.debug(f"[SearchId:${sq.searchId}]\n" + StatViewer.getPreviewString(res))
    }*/
    log.debug(f"[SearchId:${sq.searchId}]======= End command $commandname ========")
    res
  }

  def transform(_df: DataFrame): DataFrame

  def getTransform: DataFrame => DataFrame = transform

  def safeTransform(_df: DataFrame): DataFrame = {
    import java.lang.reflect.InvocationTargetException
    log.debug("Start args validating")
    validateArgs()
    log.debug("Start creating work df")
    val workDf = if (commandNotReading && commandNotPluginReading)
      buildWorkDf(_df)
    else
      _df
    Try(loggedTransform(workDf)) match {
      case Success(df) =>
        log.debug(s"transformation of ${this.getClass.getSimpleName} finished")
        df
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
        log.error(f"Error in  '$commandname' command: ${exception.getMessage}")
        throw E00014(sq.searchId, commandname, exception)
      case Failure(ex) =>
        log.error(f"Error in  '$commandname' command: ${ex.getMessage}")
        throw E00014(sq.searchId, commandname, ex)
    }
  }

  /**
   * Create dataframe with all columns used in current command
   * @param df dataframe from result of previous item of pipe
   * @return dataframe with all required columns
   */
  private def buildWorkDf(df: DataFrame): DataFrame = {
    //Defining fields, which not exists in dataframe as columns but exists in command.
    val notMakedFields: List[String] = fieldsUsed.map(_.stripBackticks).diff(df.columns.map(_.stripBackticks()))
      .filterNot(cln => cln == "+" || cln.contains("*"))
    notMakedFields match {
      case head :: _ =>
        if (df.columns.contains("_raw")) {
          import ot.scalaotl.static.FieldExtractor

          val extractor = new FieldExtractor
          extractor.makeFieldExtraction(df, notMakedFields, FieldExtractor.extractUDF)
        } else
        //Otherwise (dataframe without raw) create not-maked columns as null columns
          notMakedFields.foldLeft(df) { (acc, col) => acc.withColumn(col, lit(null)) }
      case _ => df
    }
  }
}
