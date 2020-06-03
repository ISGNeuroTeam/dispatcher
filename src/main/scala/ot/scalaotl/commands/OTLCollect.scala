package ot.scalaotl
package commands

import ot.scalaotl.config.OTLIndexes
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.types.StructField


class OTLCollect(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLIndexes {
  val requiredKeywords= Set("index")
  val optionalKeywords= Set("host","source","sourcetype")

  override def transform(_df: DataFrame): DataFrame = {
    val isTimed = _df.schema.fields.exists {
      case StructField("_time", _, _, _) => true
      case _ => false
    }

    val hasRaw = _df.schema.fields.exists {
      case StructField("_raw", _, _, _) => true
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
       // var res = keywordsMap.foldLeft(_df){case (df, (_,Keyword(k,v))) => df.withColumn(k,lit(v))}
        var res = keywordsMap.get("host") match {
          case Some(Keyword(_,v)) => _df.withColumn("_host",lit(v))
          case _=>_df
        }
        res = keywordsMap.get("source") match {
          case Some(Keyword(_,v)) => res.withColumn("_source",lit(v))
          case _=> res
        }
        res = keywordsMap.get("sourcetype") match {
          case Some(Keyword(_,v)) => res.withColumn("_sourcetype",lit(v))
          case _=> res
        }
        res = if(!isTimed) res.withColumn("_time",lit(sq.tws * 1000L)) else res
        val expr = res.schema.fields.foldLeft(List[Column]()){ (accum, item) => lit(item.name) :: lit("=") :: col(item.name) :: (lit(",") :: accum) }.dropRight(1)

        res = if(true) res.withColumn("_%raw", concat(expr: _* )) else res//!hasRaw
        res.write.parquet(f"$fsdisk$indexPathDisk/$i/bucket$timeMin-$timeMax-${System.currentTimeMillis / 1000}")
        res
      case _ =>
        log.error("Required argument 'index' not found")
        throw new CustomException(4, sq.searchId, "Required argument 'index' not found",List(commandname, "index"))
        //_df
    }
  }
}
