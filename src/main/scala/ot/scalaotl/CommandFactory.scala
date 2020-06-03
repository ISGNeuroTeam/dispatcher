package ot.scalaotl

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.lang.reflect.InvocationTargetException
import java.net.URL
import java.nio.file.Paths

import org.apache.log4j.{Level, Logger}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.proxy.{PluginBaseUtils, PluginProxyCommand}
import ot.scalaotl.commands.OTLBaseCommand

import scala.reflect.internal.util.ScalaClassLoader
import scala.util.{Failure, Success, Try}


object CommandFactory extends OTLSparkSession {
  val log: Logger = Logger.getLogger("CommandFactory")
  log.setLevel(Level.INFO)

  val pluginsDir = Try(ot.AppConfig.config.getString("plugins.path")) match{
    case Success(path) => Some(new File(Paths.get(path).toAbsolutePath.toString))
    case Failure(_) => None
  }
  var commandMap: Map[String,String] = Map()
  var jars: List[URL] = _
  var classLoader: ScalaClassLoader = _
  loadCommandsInfo(-1)

  def getCommandInstance(classname: String, param: SimpleQuery): OTLBaseCommand ={
    val x: Class[Nothing] = classLoader.tryToInitializeClass(classname).getOrElse(
      throw CustomException(-1, param.searchId, s"Class with name '$classname' is not found in classpath")
    )
    val jarPath: String = x.getProtectionDomain.getCodeSource.getLocation.getPath
    try{
      if(x.getGenericSuperclass.getTypeName.endsWith("BaseCommand")) {
        x.getConstructor(classOf[SimpleQuery]).newInstance(param)
      } else {
        val utils: PluginUtils = new PluginBaseUtils(spark, jarPath)
        val query = ot.dispatcher.sdk.core.SimpleQuery(param.args,
          param.searchId,
          param.cache,
          param.subsearches,
          param.tws,
          param.tws,
          param.searchTimeFieldExtractionEnables,
          param.preview)
        val pc: PluginCommand = x.getConstructor(classOf[ot.dispatcher.sdk.core.SimpleQuery],classOf[PluginUtils]).newInstance(query, utils)
        new PluginProxyCommand(pc, param)
      }
    } catch {
      case ex: InvocationTargetException => throw ex.getTargetException
      case ex: Throwable => throw ex
    }
  }

  def getCommand(name: String, sq: SimpleQuery): OTLBaseCommand = {
    val className = commandMap.get(name) match {
      case Some(t) => t
      case None => throw CustomException(-1, sq.searchId, s"Command with name '$name' is not found")
    }
    getCommandInstance(className, sq)
  }

  def loadCommandsInfo(id: Int): Unit ={
    import scala.collection.JavaConversions._
    val pluginsDirs: List[File] = pluginsDir.flatMap(dir => Option(dir.listFiles().filter(_.isDirectory).toList)).getOrElse(List())
    jars =  pluginsDirs.flatMap(dir => dir.listFiles.filter(f => f.isFile && f.getName.endsWith(".jar"))
        .map(_.toURI.toURL).toList)
    log.info(s"[SearchId:$id] Jars found:" + jars.mkString("[",", ", "]"))
    //Append plugin libraries to spark session. Needed to udf working on executors
    val libJars =  pluginsDirs.flatMap(dir => dir.listFiles
      .filter(d => d.isDirectory && d.getName=="libs")
      .map(dir => dir.listFiles.filter(f => f.isFile && f.getName.endsWith(".jar")).map(f =>  f.toURI.toURL))).flatten
    log.info(s"[SearchId:$id] Library jars found:" + libJars.mkString("[",", ", "]"))
    jars ++= libJars
    jars.foreach(url => spark.sparkContext.addJar(url.getPath))
    classLoader = ScalaClassLoader.fromURLs(jars, this.getClass.getClassLoader)
    log.info(s"[SearchId:$id] Jars added to driver classloader:" + jars.mkString("[",", ", "]"))
    val files = classLoader.getResources("commands.properties").toList
    val allFiles = moveToFront((i: URL) => !i.toExternalForm.contains(pluginsDir), files)//First add the file with main commands

    commandMap = allFiles.foldLeft(Map[String, String]()) {(acc, file) =>
      log.info(s"[SearchId:$id] Loading commnds from file '$file'" )
      acc ++ readCommandsMap(file, id)
    }
  }

  def readCommandsMap(url: URL, id: Int) ={
    import java.util._
    import scala.collection.JavaConverters._
    val p = new Properties()
    val is = url.openStream()
    Try(p.load(is)) match {
      case _ => is.close() // Always close resource
    }
    val res = p.asScala.toMap
    log.info(s"[SearchId:$id] Loaded commands: " + res.keySet.mkString("[",", ", "]") )
    res
  }

  def moveToFront(select: (URL) => Boolean, xs: List[URL]): List[URL] = {
    xs.span(select(_)) match {
      case (as, h::bs) => h :: as ++ bs
      case _           => xs
    }
  }
}
