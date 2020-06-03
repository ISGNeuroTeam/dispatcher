package ot.scalaotl
package commands

import ot.scalaotl.extensions.StringExt._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ sum, expr, when, col }

class OTLHead(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("limit")
  override def fieldsUsed: List[String] = List()

  override def transform(_df: DataFrame): DataFrame = {
    def isAllDigits(x: String) = x.forall(Character.isDigit)
    val lim = getKeyword("limit").getOrElse("10").toInt
    if (returns.flatFields.isEmpty) {
      _df.limit(lim)
    } else {
      val expression = returns.flatFields.mkString(" ").stripBackticks
      if (isAllDigits(expression)) {
        _df.limit(expression.toInt)
      } else {
        val ws = Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        _df.withColumn("cnt", sum(when(expr(expression), 0).otherwise(1)).over(ws))
          .filter(col("cnt") === 0)
          .drop("cnt")
          .limit(lim)
      }
    }
  }
}
