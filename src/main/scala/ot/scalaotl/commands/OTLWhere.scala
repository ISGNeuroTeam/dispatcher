package ot.scalaotl
package commands

import ot.scalaotl.parsers.ExpressionParser
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.static.{EvalFunctions, OtHash}
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.ColumnExt._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

class OTLWhere(sq: SimpleQuery) extends OTLBaseCommand(sq) with ExpressionParser {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set.empty[String]
  override def validateArgs = {
    if (args.isEmpty)
      throw CustomException(7, sq.searchId, s"Command ${commandname} shoud have at least one expression", List(commandname))
  }

  // Override from ExpressionParser to get rid of obstructive splitting by '='
  override def returnsParser = (args: String, _) => {
    val fixedArgs = EvalFunctions.argsReplace(args)
    Return(
      fields = List(),
      funcs = List(),
      evals = List(StatsEval(
        newfield = "",
        expr = fixedArgs.trim)
      )
    )
  }

  def contains = udf((v :Any, a :Seq[Any]) => a.contains(v))
  spark.udf.register("contains", contains)


  override def transform(_df: DataFrame): DataFrame = {
    returns.evals.foldLeft(_df) {
      case (acc, StatsEval(_, exp)) =>
        val pairs: List[String] =
          exp
            .split("( AND | and | OR | or )")
            .toList.map { expr =>
            val pair = """^[\(| ]* *([^\)]*)\)*$""".r.replaceAllIn(expr, """$1""")
            if (pair.contains("(") && !pair.contains(")")) s"$pair)" else pair
          }

        val replExpr = pairs.foldLeft(exp)((accExpr, ex) => {
          log.debug(s"Expression: $ex")
          val vals = ex.split("(>=|<=|!=|<|>|=)").map(splitted_part => {
            log.debug(s"Splitted part: $splitted_part (isInstanceOf[String]: ${splitted_part.isInstanceOf[String]}")
            splitted_part.trim()
          }
          )
          val arrs = vals.filter(v => isArray(expr(v).expr, acc.schema))
          val svs = vals.filter(v => !isArray(expr(v).expr, acc.schema))
          if(arrs.length > 0 && (ex.contains(">") || ex.contains("<"))) accExpr.replace(ex,"false")
          else
          if (arrs.length == 1) {
           // val c = if (acc.schema.toList.map(_.name).contains(svs.head)) svs.head else svs.head.strip("\"")
            val containsExpr = s"contains(${svs.head}, ${arrs.head})"
            val ncExpr = if (ex.contains("!=")) "not " + containsExpr else containsExpr
            accExpr.replace(ex, ncExpr)
          }else {

            accExpr.replace(ex, ex.withPeriodReplace)
          }})
        acc.filter( expr(replExpr).withExtensions(acc.schema))
    }
  }

}
