package ot

import java.io.{File, FileInputStream}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import ot.scalaotl.extensions.StringExt._

/** Loads config from application.conf or file from spark.application.config.
 *
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
object AppConfig {
  var logLevels: Map[String, Properties] = Map()

  def getLogLevel(config: Config, path: String): String = {
    getLogLevel(config, path, "main")
  }

  def getLogLevel(config: Config, path: String, pluginName: String): String = {
    if (config.hasPath("loglevel") && new File(config.getString("loglevel")).exists) {
      val props = logLevels.get(pluginName) match {
        case Some(p) => p
        case None => val p = new Properties()
          val logLevelPath = config.getString("loglevel")
          val is = new FileInputStream(logLevelPath)
          p.load(is)
          logLevels = logLevels.updated(pluginName, p)
          p
      }
      props.getProperty(path, "WARN").strip("\"")
    } else "WARN"
  }

  val log: Logger = Logger.getLogger("AppConfigLogger")
  log.setLevel(Level.toLevel("INFO"))
  log.info("Loading default config from resources in jar.")
  val baseConfig: Config = ConfigFactory.load()
  var config: Config = baseConfig

  /** Merges config with one of loading from specified or hardcoded path.
   *
   * @param fileConfigOption spark.application.config value from submit options.
   */
  def updateConfigWith(fileConfigOption: Option[String]): Unit = {
    fileConfigOption match {
      case Some(fileConfig) =>
        log.info("Merging config with one from specified external path if it exists.")
        config = ConfigFactory.parseFile(new File(fileConfig)).withFallback(baseConfig)
      case None =>
        log.info("Merging config with one from hardcoded external path if it exists.")
        config = ConfigFactory.parseFile(new File("/mnt/glfs/configs/dispatcher/application.conf"))
          .withFallback(baseConfig)
    }
    log.setLevel(Level.toLevel(getLogLevel(config, "appconfig"))) //TODO
    log.debug(s"Configuration: ${config.toString}")
  }
}
