package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import ot.scalaotl.parsers.StatsParser
import ot.scalaotl.static.EvalFunctions

import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.extensions.DataFrameExt._
import org.apache.spark.sql.functions.{ col, from_unixtime }

class OTLConvert(sq: SimpleQuery) extends OTLBaseCommand(sq) with StatsParser {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("timeformat")

  override def returnsParser = (args: String, seps: Set[String]) => {
    val argsFiltered = excludeKeywords(excludePositionals(args, seps), keywordsParser(args))
    val (evals, argsNoEvals) = parseEvals(argsFiltered, "num|ctime")
    val parsed = rexSimpleStatsFunc("num|ctime").findAllIn(args).matchData.map(x => {
      val oldf = Option(x.group(3)).getOrElse("__fake__").trim
      val func = x.group(1).trim
      val newf = Option(x.group(5)).getOrElse(oldf).trim
      StatsFunc(newf, func, oldf)
    }).toList
    Return(fields = List(),parsed, evals)
  }

  override def transform(_df: DataFrame): DataFrame = {
    val timeformat = getKeyword("timeformat").getOrElse("YYYY-MM-DD HH:mm:ss").replaceByMap(EvalFunctions.dateFormatOTPToJava)
    val initCols = _df.columns
    returns.funcs.foldLeft(_df) {
      case (accum, StatsFunc(newfield, func, field)) => {
        val f = func match {
          case "num"   => col(field).cast("double")
          case "ctime" => from_unixtime(col(field), timeformat)
          case _       => col(field).cast("string")
        }
        val res = if (initCols.contains(field)) accum.withColumn(newfield, f) else accum
        res
      }
    }
  }
}
