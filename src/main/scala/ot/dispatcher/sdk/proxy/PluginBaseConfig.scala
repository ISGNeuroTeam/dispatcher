package ot.dispatcher.sdk.proxy

import java.net.URL
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import ot.AppConfig
import ot.dispatcher.sdk.PluginConfig
import ot.scalaotl.CustomException

import scala.util.{Failure, Success, Try}

class PluginBaseConfig(jarPath: String) extends PluginConfig{
  override def pluginConfig: Config = configuration
  override def mainConfig: Config = AppConfig.config
  def baseConfiguration = ConfigFactory.parseURL(getDefaultConfigUrl())
  def pluginName: String = Try(baseConfiguration.getString("pluginName")) match {
    case Success(n) => n
    case Failure(_) => throw CustomException(0, -1, "pluginName variable not found in plugin.conf inside jar")
  }
  def configuration = Try(ot.AppConfig.config.getString("plugins.path")) match{
    case Success(path) =>
      val nconf = ConfigFactory.parseURL(Paths.get(path, pluginName, "plugin.conf").toAbsolutePath.toUri.toURL)
      nconf.withFallback(baseConfiguration)
    case Failure(_) => baseConfiguration
  }
  def getLoglevel(name: String) = {
    AppConfig.getLogLevel(configuration, name, pluginName)
  }

  def getDefaultConfigUrl() = {
//    val jarPath = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    if(jarPath.toString.endsWith("classes/")){
      //Code run from plugin tests. Use only one plugin a time
      this.getClass.getClassLoader.getResource("plugin.conf")
    } else {
      //Plugins are loaded from jar file. The number of plugins is not limited
      val confPathStr = Paths.get(s"jar:file:$jarPath!", "plugin.conf").toString
      new URL(confPathStr)
    }
  }
}
