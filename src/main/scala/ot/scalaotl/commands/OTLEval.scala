package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import ot.dispatcher.sdk.core.CustomException.E00020
import ot.scalaotl.extensions.ColumnExt._
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.ExpressionParser

class OTLEval(sq: SimpleQuery) extends OTLBaseCommand(sq) with ExpressionParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def getFieldsUsed: Return => List[String] = (ret: Return) => {
    ret.evals.flatMap {
      case x if x.expr.isEmpty => List.empty[String]
      case x => getFieldsFromExpression(expr(x.expr).expr, List()).map(_.stripBackticks().addSurroundedBackticks)
    }
  }

  override def validateArgs(): Unit = {
    if (keywordsMap.isEmpty)
      throw E00020(sq.searchId, commandname)
  }

  override def transform(_df: DataFrame): DataFrame = {
    log.debug("Start eval")
    val sch = _df.schema
    log.debug("Start foldLeft")
    returns.evals.foldLeft(_df)((acc, eval) => eval match {
      case StatsEval(newfield, expression) =>
        log.debug("Start item")
        acc
        .withColumn(newfield.strip("\"").strip("\'"), expr(expression.replaceFirst("""`\Q""" + newfield + """\E` *=""", "")
          .withPeriodReplace()).withExtensions(sch))
      case _ => acc
    }
    )
  }
}
