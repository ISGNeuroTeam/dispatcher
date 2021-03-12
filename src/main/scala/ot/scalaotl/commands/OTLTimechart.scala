package ot.scalaotl
package commands

import ot.scalaotl.static.OtDatetime
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.StatsParser
import ot.dispatcher.OTLQuery

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

class OTLTimechart(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) with StatsParser {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("span")

  override val fieldsUsed = getFieldsUsed(returns) ++ getPositionalFieldUsed(positionals)
  override val fieldsGenerated = getFieldsGenerated(returns)

  override def transform(_df: DataFrame): DataFrame = {    
    val span = OtDatetime.getSpanInSeconds(getKeyword("span").getOrElse("1d"))
    log.debug(f"[SearchId:${sq.searchId}] Counted span in seconds: $span")
    val dfTime = _df.withColumn("_time", expr(s"""floor(_time / $span) * $span""").cast("long"))

    val argsClear = keywordsMap.foldLeft(args.split("by").toList.headOption.getOrElse("")) {
      case (accum, (_, Keyword(k, v))) => accum.replaceAllLiterally(s"$k=$v", "").trim
    }

    val by = getPositional("by") match {
      case Some(List()) | None => ""
      case Some(list)          => s"""by ${list.map(_.stripBackticks).mkString(" ")}"""
    }
    val chartArgs = s"$argsClear over _time $by".trim
    log.debug(f"[SearchId:${sq.searchId}] Args for chart: $chartArgs")
    new Converter(OTLQuery(s"chart $chartArgs | sort _time")).setDF(dfTime).run
  }
}
