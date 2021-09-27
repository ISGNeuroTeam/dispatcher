package ot.dispatcher

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, NullType, StringType, StructField, StructType, ArrayType, BooleanType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.AppConfig._
import ot.dispatcher.sdk.core.CustomException.{E00007, E00011}
import ot.dispatcher.sdk.core.CustomException

import scala.reflect.io.File

/** Makes all manipulations with Jobs result caches.
  * [[makeCache]] - Limits and saves result [[DataFrame]] to RAM cache.
  * [[removeCache]] - Remove cache files from RAM cache.
  * [[loadCache]] - Loads [[DataFrame]] from RAM cache.
  *
  * @param sparkSession [[SparkSession]] for loading cache.
  * @author Andrey Starchenkov (astarchenkov@ot.ru)
  */
class CacheManager(sparkSession: SparkSession) {

  val log: Logger = Logger.getLogger("CacheManagerLogger")
  log.setLevel(Level.toLevel(getLogLevel(config,"cachemanager")))

  // Loads settings for RAM cache from config.
  val fs: String = config.getString("memcache.fs")
  val path: String = config.getString("memcache.path")
  val maxRows = config.getInt("indexes.max_rows")

  /** Saves result [[DataFrame]] to RAM cache.
    * Before saving sets format and limits to cache, writes the scheme of DF.
    *
    * @param df Resulting [[DataFrame]].
    * @param id Job ID.
    */
  def makeCache(df: DataFrame, id: Int): Unit = {
    log.debug(s"Job $id. Cache: $fs$path" + s"search_$id.cache. Schema: ${df.schema}.")
    try {
      df.limit(maxRows).write
        .format("json")
        .save(s"$fs$path" + s"search_$id.cache/data")
    }catch {
      case ex: Exception => throw makeCustomException(ex, id)
    }

    File(s"$path" + s"search_$id.cache/data/_SCHEMA").writeAll(df.schema.toDDL)
    log.debug(s"Job $id. Cache: $fs$path" + s"search_$id.cache is written.")
  }

  private def makeCustomException(ex: Exception, id: Int)= {
    val exception = ex match {
      case e0 if !e0.getMessage.equals("Job aborted.") => e0
      case e0 => e0.getCause match {
        case e1 if !e1.getMessage.startsWith("Job aborted due to stage failure") => e1
        case e1 => e1.getCause match {
          case e2 if !e2.getMessage.startsWith("Failed to execute user defined function") => e2
          case e2 => e2.getCause
        }
      }
    }
    log.error(f"Runtime error: ${exception.getMessage}" )
    E00007(id, exception.getMessage, exception)
  }

  /** Removes cache files from RAM cache.
    *
    * @param id Cache ID.
    */
  def removeCache(id: Int): Unit = {
    import scala.reflect.io.File
    val pathToCache = s"$path" + s"search_$id.cache/"
    val dir = File(pathToCache)
    val status = dir.deleteRecursively()
    log.debug(s"Cache path: $pathToCache")
    log.debug(s"Cache $id was deleted. State: $status.")
  }

  /** Loads [[DataFrame]] from RAM cache.
    * Before loading sets format of saved data and checks if header is present.
    *
    * @param id Cache ID.
    * @return [[DataFrame]] with cache data.
    */
  def loadCache(id: Int): DataFrame = {

    val schema = File(s"${path}search_$id.cache/data/_SCHEMA").bufferedReader().readLine()

    val structType = StructType.fromDDL(schema)
    
    log.debug(s"Subsearch $id schema: $schema.")
    val df = sparkSession.read
      .format("json")
      .schema(structType)
      .load(s"$fs$path" + s"search_$id.cache/data")
    log.debug(s"Loaded cache $id.")
    df
  }

  def clearCacheDirectory(): Unit = {
    import scala.reflect.io.Directory
    val dirCaches = Directory(path)
    dirCaches.list.foreach(_.deleteRecursively())
  }
}
