package ot.scalaotl
package commands

import com.isgneuro.otl.processors.Where
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import ot.dispatcher.sdk.core.CustomException.E00022
import ot.scalaotl.parsers.ExpressionParser
import ot.scalaotl.static.EvalFunctions

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
    val exprs = returns.evals.map(_.expr)
    val worker = new Where(spark, exprs, log)
    worker.transform(_df)
  }

}
