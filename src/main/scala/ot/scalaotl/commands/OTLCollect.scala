package ot.scalaotl
package commands

import org.apache.log4j.Level
import ot.scalaotl.config.OTLIndexes
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.types.StructField
import ot.scalaotl.static.OtDatetime
import ot.scalaotl.utils.logging.StatViewer
import ot.dispatcher.sdk.core.CustomException.E00019

/*/**
 * Adds the results of a search to an index that you specify.
 * ==Syntax==
 * | collect collect index=__index__ [source=__source__] [sourcetype=__sourcetype__] [host=__host__]
 * | collect index=__index__ mode=__mode__
 *
 * ==Arguments==
 * mode[String] - Mode of working with DataFrame. Default: raw.
 * raw - Aggregates all columns to _raw column and appends it ot DataFrame.
 * as-is - Writes DataFrame to buckets as it is, without any modifications.
 * source[String] - Updates original value of column source with new one. It's used only in raw mode.
 * sourcetype[String] - Updates original value of column sourcetype with new one. It's used only in raw mode.
 * host[String] - Updates original value of column host with new one. It's used only in raw mode.
 *
 * @param sq SimpleQuery object with search information.
 */*/
/** =Abstract=
 * This class provides support of __'''collect'''__ otl command.
 *
 * __'''collect'''__ stores the results of a query in the specified index.
 *
 * __'''collect'''__ takes one reqired argument __index__ - the name (any word) of the index where the dataset will be stored,
 * and three optional arguments:
 *
 * __mode__ - if it equals __'''as-is'''__, then the unchanged dataset will be saved,
 * with any other value it does not affect in any way
 *
 * __host__ - if this argument is given, then the dataset will be saved with the '''_host''' field
 * equal to the value of this argument
 *
 * __source__ - if this argument is given, then the dataset will be saved with the '''_source''' field
 * equal to the value of this argument
 *
 * __sourcetype__ - if this argument is given, then the dataset will be saved with the '''_sourcetype''' field
 * equal to the value of this argument
 *
 *
 * =Usage example=
 * ==Example 1==
 * OTL:
 * {{{| makeresults count = 5 | eval a=1 | eval b=2
 *| collect index=collect_test source=test sourcetype=test host=test_machine}}}
 *
 * Result:
 *{{{+----------+---+---+------------+-------+-----------+--------------------+
|     _time|  a|  b|       _host|_source|_sourcetype|                _raw|
+----------+---+---+------------+-------+-----------+--------------------+
|1645719910|  1|  2|test_machine|   test|       test|_sourcetype=test,...|
|1645719910|  1|  2|test_machine|   test|       test|_sourcetype=test,...|
|1645719910|  1|  2|test_machine|   test|       test|_sourcetype=test,...|
|1645719910|  1|  2|test_machine|   test|       test|_sourcetype=test,...|
|1645719910|  1|  2|test_machine|   test|       test|_sourcetype=test,...|
+----------+---+---+------------+-------+-----------+--------------------+}}}
 *
 * ==Example 2==
 * OTL:
 * {{{| makeresults count = 5 | eval a=1 | eval b=2
 *| collect index=collect_test source=test sourcetype=test host=test_machine mode=as-is}}}
 * Result:
 * {{{+----------+---+---+
|     _time|  a|  b|
+----------+---+---+
|1645720466|  1|  2|
|1645720466|  1|  2|
|1645720466|  1|  2|
|1645720466|  1|  2|
|1645720466|  1|  2|
+----------+---+---+}}}
 *
 * @constructor creates new instance of [[OTLCollect]]
 * @param sq [[SimpleQuery]]
 */
class OTLCollect(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLIndexes {
  val requiredKeywords: Set[String] = Set("index")
  val optionalKeywords: Set[String] = Set("host", "source", "sourcetype")

  override val fieldsUsed: List[String] = List()

  /**
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return _df that was stored
   * @throws E00019 [[ot.dispatcher.sdk.core.CustomException.E00019]]
   */
  override def transform(_df: DataFrame): DataFrame = {
    if (log.getLevel == Level.DEBUG) {
      log.debug(f"[SearchId:${sq.searchId}] Input:\n" + StatViewer.getPreviewString(_df))
    }

    val isTimed = _df.schema.fields.exists {
      case StructField("_time", _, _, _) => true
      case _ => false
    }

    val (timeMin, timeMax) = if (isTimed) {
      val times = _df.agg("_time" -> "min", "_time" -> "max").collect()(0)
      (times.getAs[Long]("min(_time)") / 1000, times.getAs[Long]("max(_time)") / 1000)
    } else {
      val time = if (sq.tws != 0) sq.tws else System.currentTimeMillis / 1000
      (time, time)
    }

    val res = keywordsMap.get("index") match {
      case Some(Keyword(_, indexName)) =>
        val res = keywordsMap.get("mode") match {
          case Some(Keyword(_, "as-is")) => _df
          case _ =>
            var raw_df = keywordsMap.get("host") match {
              case Some(Keyword(_, v)) => _df.withColumn("_host", lit(v))
              case _ => _df
            }
            raw_df = keywordsMap.get("source") match {
              case Some(Keyword(_, v)) => raw_df.withColumn("_source", lit(v))
              case _ => raw_df
            }
            raw_df = keywordsMap.get("sourcetype") match {
              case Some(Keyword(_, v)) => raw_df.withColumn("_sourcetype", lit(v))
              case _ => raw_df
            }
            raw_df = if (!isTimed) raw_df.withColumn("_time", lit(OtDatetime.getCurrentTimeInSeconds())) else raw_df
            val expr = raw_df.schema.fields.foldLeft(List[Column]()) { (accum, item) => lit(item.name) :: lit("=") :: col(item.name) :: (lit(",") :: accum) }.dropRight(1)
            raw_df.withColumn("_raw", concat(expr: _*))
        }
        res.write.parquet(f"$fsdisk$indexPathDisk/$indexName/bucket-${timeMin * 1000}-${timeMax * 1000}-${System.currentTimeMillis / 1000}")
        res

      case _ =>
        log.error("Required argument 'index' not found")
        throw E00019(sq.searchId, commandName)
    }
    res.show()
    res
  }
}
