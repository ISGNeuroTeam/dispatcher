package ot.dispatcher.sdk.proxy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.OTLQuery
import ot.dispatcher.sdk.PluginUtils
import ot.scalaotl.Converter
import ot.dispatcher.sdk.core.CustomException.E00002
import ot.scalaotl.utils.logging.StatViewer

class PluginBaseUtils(sparkSession: SparkSession, jarPath: String) extends PluginBaseConfig(jarPath) with PluginUtils {

  override def getLoggerFor(classname: String): Logger = {
    val log = Logger.getLogger(classname)
    val simpleName = classname.substring(classname.lastIndexOf('.'))
    log.setLevel(Level.toLevel(logLevelOf(simpleName)))
    log
  }

  override def logLevelOf(name: String): String = getLoglevel(name)

  override def printDfHeadToLog(log: Logger, id: Int, df: DataFrame): Unit = if (log.getLevel == Level.DEBUG) {
    log.debug(f"[SearchId:$id]\n" + StatViewer.getPreviewString(df))
  }

  override def sendError(id: Int, message: String) = throw E00002(id, message)

  override def spark: SparkSession = sparkSession

  override def executeQuery(query: String, df: DataFrame): DataFrame = new Converter(OTLQuery(query)).setDF(df).run

  override def executeQuery(query: String, index: String, startTime: Int, finishTime: Int): DataFrame = {
    val otlQuery = new OTLQuery(
      id = -1,
      original_otl = s"genrated by command",
      service_otl = s""" | otstats {"$index": {"query": "", "tws": "$startTime", "twf": "$finishTime"}} | $query """,
      tws = startTime,
      twf = finishTime,
      cache_ttl = 0,
      indexes = Array(index),
      subsearches = Map(),
      username = "internal",
      field_extraction = false,
      preview = false
    )
    new Converter(otlQuery).run
  }
}
