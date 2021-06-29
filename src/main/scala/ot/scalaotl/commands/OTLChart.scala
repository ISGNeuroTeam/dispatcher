package ot.scalaotl
package commands

import ot.scalaotl.parsers._
import ot.scalaotl.static.StatsFunctions
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.StringExt._

import org.apache.spark.sql.DataFrame

case class OverBy(over: List[String], by: List[String])

class OTLChart(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by", "over")) with StatsParser with WildcardParser {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set.empty[String]

  override val fieldsUsed: List[String] = getFieldsUsed(returns) ++ getPositionalFieldUsed(positionals)
  override val fieldsGenerated: List[String] = getFieldsGenerated(returns)

  override def transform(_df: DataFrame): DataFrame = {
    val sortNeeded = returns.funcs.map(_.func).intersect(List("earliest", "latest")).nonEmpty
    val dfSorted = if (sortNeeded) _df.orderBy("_time") else _df

    // Calculate evaluated field. Add __fake__ column for possible 'count' function
    val dfWithEvals = StatsFunctions.calculateEvals(returns.evals, dfSorted).withFake

    // Replace wildcards with actual column names
    val cols: Array[String] = dfWithEvals.columns
    val returnsWcFuncs: List[StatsFunc] = returnsWithWc(cols, returns).funcs

    val statsFunctions = StatsFunctions.getExpr(returnsWcFuncs)

    val overfield = getPositional("over").getOrElse(List())
    val byfield = getPositional("by").getOrElse(List())
    val overby = OverBy(overfield, byfield)

    val dfg = overby match {
      case OverBy(List(), List()) => dfWithEvals.groupBy("__fake__")
      case OverBy(overhead :: overtail, List()) => dfWithEvals.groupBy(overhead)
      case OverBy(List(), byhead :: bytail) => dfWithEvals.groupBy(byhead)
      case OverBy(overhead :: overtail, byhead :: bytail) => dfWithEvals.groupBy(overhead).pivot(byhead)
    }

    val dfres = statsFunctions match {
      case sfHead :: sfTail => dfg.agg(sfHead, sfTail: _*).dropFake
      case _ => _df
    }    

    // Rename columns to OTP format: from <BYVAL>_<FUNC> to <FUNC>: <BYVAL>. If needless, remove the block below and return `dfres`
    val newfields = returns.funcs.map(x => x.newfield.stripBackticks())
    val resCols = dfres.columns
    newfields.flatMap {
      nf => {
        resCols.filter(_.endsWith(s"_$nf"))
          .map(colName => {
            val byOnly = colName.replaceAllLiterally(s"_$nf", "")
            colName -> s"$nf: $byOnly"
          })
      }
    }
    .foldLeft(dfres){
      case (acc, (oldf, newf)) => acc.withSafeColumnRenamed(oldf, newf)
    }
  }
}
