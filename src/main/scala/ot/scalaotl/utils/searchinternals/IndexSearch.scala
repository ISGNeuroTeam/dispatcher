package ot.scalaotl
package utils
package searchinternals

import ot.scalaotl.config.OTLIndexes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions => F}
import org.apache.spark.sql.types.LongType

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.AnalysisException
import org.apache.commons.lang.StringEscapeUtils.escapeJava
import ot.AppConfig.{config, getLogLevel}
import ot.scalaotl.extensions.DataFrameExt._
import ot.dispatcher.sdk.core.CustomException.{E00003, E00004, E00005, E00006}
import ot.dispatcher.sdk.core.CustomException

class IndexSearch(spark: SparkSession, item: (String, Map[String, String]), searchId: Int,
                  fieldsUsedInFullQuery: Seq[String], preview: Boolean, fullReadFlag: Boolean = false) extends OTLIndexes
{
  val log: Logger = Logger.getLogger(this.getClass.getName)
  log.setLevel(Level.toLevel(getLogLevel(config, this.getClass.getSimpleName)))


  val indexName: String = item._1
  val query: String = item._2("query")
  // timestamp of the start of the searched data
  val _tws: Long = item._2("tws").toLong
  // timestamp of the finish of the searched data
  val _twf: Long = item._2("twf").toLong
  val tws: Long = _tws match {
    case 0 => Long.MinValue
    case _ => _tws
  }
  val twf: Long = _twf match {
    case 0 => Long.MaxValue
    case _ => _twf
  }


  def getMergedDf(df_disk: DataFrame, df_cache: DataFrame): DataFrame = {
    val jdf = df_cache.agg(F.min("_time")).na.fill(Long.MaxValue, Seq("min(_time)"))
    val df_disk_filter = df_disk
      .crossJoin(jdf)
      .filter(F.col("_time") < F.col("min(_time)"))
      .drop("min(_time)")
    // in the current version of the dispatcher, it is assumed that the index schema is unchanged
    // that is, the index data on disk and in the cache cannot have a different schemas
//    val cols1 = df_disk_filter.columns.toSet
//    val cols2 = df_cache.columns.toSet
//    val total = cols1 ++ cols2
//        def expr(myCols: Set[String], allCols: Set[String]) = {
//          allCols.toList.map(x => { if (myCols.contains(x)){F.col(x)} else F.lit(null).as(x)})
//        }
//         df_disk_filter.select(expr(cols1, total): _*).union(df_cache.select(expr(cols2, total): _*))
//    df_cache.append(df_disk_filter)
    df_disk_filter
  }

  def getException(ex1: Throwable, ex2: Throwable): Throwable = {
    (ex1, ex2) match {
      case (ex1: AnalysisException, _) => E00003(searchId, ex1.getMessage())
      case (e1: CustomException, e2: CustomException) => E00004(searchId, indexName)
      case ex => E00005(searchId, indexName, ex._1.getMessage, ex._1)
    }
  }

  def search(): DataFrame = {
    log.debug(s"[SearchID:$searchId] IndexSearch: indexPathDisk ='$indexPathDisk'; indexPathCache = '$indexPathCache'")
    var result_disk: Try[DataFrame] = Failure(E00006(searchId))
    val delta = System.currentTimeMillis - tws * 1000
    val duration_cache_millis = durationCache * 1000
    log.info(s"[SearchID:$searchId] IndexSearch: indexPathDisk ='$indexPathDisk'; " +
      s"indexPathCache = '$indexPathCache'; tws = '$tws'; delta = '$delta'; " +
      s"duration_cache_millis = '$duration_cache_millis'")
    if (delta >= duration_cache_millis) {
      val search_disk = new FileSystemSearch(spark, searchId, fieldsUsedInFullQuery, fs_disk, indexPathDisk,
        indexName, query, tws, twf, preview, fullReadFlag = fullReadFlag)
      result_disk = search_disk.search()
    }
    val search_cache = new FileSystemSearch(spark, searchId, fieldsUsedInFullQuery, fs_cache, indexPathCache,
      indexName, query, tws, twf, preview, fullReadFlag = fullReadFlag, isCache = true)
    val result_cache = search_cache.search()

    (result_disk, result_cache) match {
      case (Success(disk), Success(cache)) => getMergedDf(disk, cache)
      case (Failure(disk), Success(cache)) => cache
      case (Success(disk), Failure(cache)) => disk
      case (Failure(ex_disk), Failure(ex_cache)) => throw getException(ex_disk, ex_cache)
    }
  }
}
