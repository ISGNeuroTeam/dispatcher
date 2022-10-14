package ot.scalaotl
package static

import ot.scalaotl.StatsFunc
import ot.scalaotl.commands.OTLEval
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.ColumnExt._
import ot.scalaotl.extensions.StringExt._

import scala.util.matching.Regex

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.functions.expr

object StatsFunctions {
  val funclist = List("max", "min", "dc", "distinct_count", "earliest", "latest", "count", "p\\d+", "approxdc", "avg", "mean",
    "list", "stdev", "values", "var", "first", "last", "sum")
  val funclistString = funclist.mkString("|")

  var exprSwitcher: Map[String, String] = Map(
    ("approxdc", "approx_count_distinct"),
    ("avg", "mean"),
    ("earliest", "first"),
    ("latest", "last"),
    ("list", "collect_list"),
    ("perc", "percentile_approx"),
    ("stdev", "stddev"),
    ("values", "collect_set"),
    ("var", "variance"))

  def getExpr(func: String, field: String, newfield: String = ""): Column = {
    val rexPerc: Regex = raw"^p\d+".r
    val exprStr = func match {
      case "median" => s"percentile_approx($field, 0.5, 10)"
      case rexPerc(_*) => {
        val p = func.stripPrefix("perc").stripPrefix("p").toDouble / 100
        s"percentile_approx($field, $p, 10)"
      }
      case _ => s"${exprSwitcher.getOrElse(func, func)}($field)"
    }

    val exprStrAlias = if (newfield.nonEmpty)
      exprStr + s" as $newfield"
    else exprStr

    expr(exprStrAlias).withDc
  }

  def getExpr(funcs: List[StatsFunc]): List[Column] = {
    funcs.map {
      case StatsFunc(newcolname, funcname, colname) => getExpr(funcname, colname, newcolname)
    }
  }

  def calculateEvals(evals: List[StatsEval], df: DataFrame) = {
    evals.foldLeft(df) {
      case (accum, eval) => new OTLEval(SimpleQuery(s"${eval.newfield} = ${eval.expr}")).transform(accum)
    }
  }

  def applyFuncs(funcs: List[StatsFunc], df: DataFrame, groups: List[String] = List()) = {
    val fs = funcs.filterNot{
      case StatsFunc(_, _, field) => groups.contains(field)
    }.map {
      case StatsFunc(newfield, func, field) => StatsFunctions.getExpr(func, field, newfield)
    }
    df.complexAgg(groups, fs)
  }
}
