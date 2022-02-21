package ot.scalaotl.commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import ot.scalaotl.{Return, SimpleQuery, StatsEval}
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.ExpressionParser


class SprkCommand(sq: SimpleQuery) extends OTLBaseCommand(sq) with ExpressionParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("value")
  val searchId: Int = sq.searchId

  override def getFieldsUsed: Return => List[String] = (ret: Return) => {
    ret.evals.flatMap {
      case x if x.expr.strip("\"").isEmpty => List.empty[String]
      case x => getFieldsFromExpression(expr(x.expr.strip("\"")).expr, List()).map(_.addSurroundedBackticks)
    }
  }

  override def transform(_df: DataFrame): DataFrame = {
    returns.evals.foldLeft(_df) {
      case (acc, StatsEval(field, ex)) =>
        _df.withColumn(field, expr(ex.strip("\"")))
    }
  }
}
