package ot.scalaotl
package commands

import com.isgneuro.otl.processors.Eval
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import ot.dispatcher.sdk.core.CustomException
import ot.dispatcher.sdk.core.CustomException.E00020
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.ExpressionParser

class OTLEval(sq: SimpleQuery) extends OTLBaseCommand(sq) with ExpressionParser {

  import spark.implicits._

  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  var errorInExpression: Option[ExpressionError] = None

  override def getFieldsUsed: Return => List[String] = (ret: Return) => {
    ret.evals.flatMap {
      case x if x.expr.isEmpty => List.empty[String]
      case x => try {
        getFieldsFromExpression(expr(x.expr).expr, List()).map(_.stripBackticks().addSurroundedBackticks)
      } catch {
        case e: Exception =>
          errorInExpression = Some(ExpressionError(e.getMessage, x.newfield))
          List.empty[String]
      }
    }
  }

  override def validateArgs(): Unit = {
    try {
      if (keywordsMap.isEmpty)
        throw E00020(sq.searchId, commandname)
    } catch {
      case e: CustomException =>
        errorInExpression = Some(ExpressionError(e.getMessage, "No fields"))
    }
  }

  override def transform(_df: DataFrame): DataFrame = {
    errorInExpression match {
      case Some(er) => Seq(
        ("Error", er.message, er.field)
      ).toDF("ResultType", "Message", "Column")
      case None => calcEval(_df)
    }
  }

  private def calcEval(_df: DataFrame): DataFrame = {
    val evalsMap = {
      for {e <- returns.evals}
        yield (e.newfield -> e.expr)
    }.toMap
    val worker = new Eval(spark, evalsMap)
    worker.transform(_df)
  }
}

case class ExpressionError(message: String, field: String)