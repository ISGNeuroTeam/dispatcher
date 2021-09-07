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

/**
 * Adds the results of a search to an index that you specify.
 * ==Syntax==
 * | collect collect index=__index__ [source=__source__] [sourcetype=__sourcetype__] [host=__host__]
 * | collect index=__index__ mode=__mode__
 *
 * ==Arguments==
 * mode[String] - Mode of working with DataFrame. Default: raw.
 *  raw - Aggregates all columns to _raw column and appends it ot DataFrame.
 *  as-is - Writes DataFrame to buckets as it is, without any modifications.
 * source[String] - Updates original value of column source with new one. It's used only in raw mode.
 * sourcetype[String] - Updates original value of column sourcetype with new one. It's used only in raw mode.
 * host[String] - Updates original value of column host with new one. It's used only in raw mode.
 *
 * @param sq SimpleQuery object with search information.
 */
class OTLCollect(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLIndexes {
  val requiredKeywords = Set("index")
  val optionalKeywords = Set("host","source","sourcetype")

  override val fieldsUsed = List()

  override def transform(_df: DataFrame): DataFrame = {
    if (log.getLevel == Level.DEBUG) {
      log.debug(f"[SearchId:${sq.searchId}] Input:\n" + StatViewer.getPreviewString(_df))
    }

    val isTimed = _df.schema.fields.exists {
      case StructField("_time", _, _, _) => true
      case _ => false
    }

    val (timeMin, timeMax) =  if(isTimed){
      val times = _df.agg("_time" -> "min","_time" -> "max").collect()(0)
      (times.getAs[Long]("min(_time)") / 1000, times.getAs[Long]("max(_time)") / 1000)
    }else{
      val time = if(sq.tws != 0) sq.tws else System.currentTimeMillis / 1000
      (time, time)
    }

    keywordsMap.get("index") match {
      case Some(Keyword(_,i)) =>
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
        res.write.parquet(f"$fsdisk$indexPathDisk/$i/bucket-${timeMin*1000}-${timeMax*1000}-${System.currentTimeMillis / 1000}")
        res

      case _ =>
        log.error("Required argument 'index' not found")
        throw E00019(sq.searchId, commandname)
    }
  }
}
