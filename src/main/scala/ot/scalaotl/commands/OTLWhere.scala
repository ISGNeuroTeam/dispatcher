package ot.scalaotl
package commands

import org.apache.spark.sql.expressions.UserDefinedFunction
import ot.scalaotl.parsers.ExpressionParser
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.static.EvalFunctions
import ot.scalaotl.extensions.ColumnExt._
import ot.dispatcher.sdk.core.CustomException.E00022
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

class OTLWhere(sq: SimpleQuery) extends OTLBaseCommand(sq) with ExpressionParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def validateArgs(): Unit = {
    if (args.isEmpty)
      throw E00022(sq.searchId, commandname)
  }

  // Override from ExpressionParser to get rid of obstructive splitting by '='
  override def returnsParser: (String, Set[String]) => Return = (args: String, _) => {
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

  def contains: UserDefinedFunction = udf((v: Any, a: Seq[Any]) => a.contains(v))

  spark.udf.register("contains", contains)


  override def transform(_df: DataFrame): DataFrame = {
    returns.evals.foldLeft(_df) {
      case (acc, StatsEval(_, exp)) =>
        val pairs = exp.split("( AND | and | OR | or )").toList.map({ p =>
          val pair = """^[\(| ]* *([^\)]*)\)*$""".r.replaceAllIn(p, """$1""")
          if (pair.startsWith("like(")) pair + ")" else pair
        })
        val replExpr = pairs.foldLeft(exp)((accExpr, ex) => {
          log.debug(s"Expression: $ex")
          val vals = ex.split("(>=|<=|!=|<|>|=)").map(splitted_part => {
            log.debug(s"Splitted part: $splitted_part (isInstanceOf[String]: ${splitted_part.isInstanceOf[String]}")
            splitted_part.trim()
          }
          )
          val arrs = vals.filter(v => isArray(expr(v).expr, acc.schema))
          val svs = vals.filter(v => !isArray(expr(v).expr, acc.schema))
          if (arrs.length > 0 && (ex.contains(">") || ex.contains("<"))) accExpr.replace(ex, "false")
          else if (arrs.length == 1) {
            // val c = if (acc.schema.toList.map(_.name).contains(svs.head)) svs.head else svs.head.strip("\"")
            val containsExpr = s"contains(${svs.head}, ${arrs.head})"
            val ncExpr = if (ex.contains("!=")) "not " + containsExpr else containsExpr
            accExpr.replace(ex, ncExpr)
          } else {
            val expression = castFieldsToStr(ex, svs, acc)
            accExpr.replace(ex, expression.withPeriodReplace())
          }
        })
        acc.filter(expr(replExpr).withExtensions(acc.schema))
    }
  }

  //converts field to string if it compares with string const
  def castFieldsToStr(ex: String, fieldsInExpr: Seq[String], df: DataFrame): String = {
    val existingFields = df.schema.toList.map(_.name).intersect(fieldsInExpr)
    val fieldsDifference = fieldsInExpr.diff(existingFields)
    val fieldsDifferenceLength = fieldsDifference.length
    val fieldIsOneDigit = fieldsDifferenceLength == 1 && fieldIsDigit(fieldsDifference.head)

    if (fieldsDifference.nonEmpty && !fieldIsOneDigit)
      existingFields.foldLeft(ex) { (acc, f) => acc.replace(existingFields.head, s"cast(${existingFields.head} as string)") }
    else ex
  }

  def fieldIsDigit(field: String): Boolean =
    field.toIntSafe match {
      case Some(_) => true
      case None => false
    }
}
