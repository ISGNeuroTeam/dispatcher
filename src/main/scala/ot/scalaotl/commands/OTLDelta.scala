package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import ot.scalaotl.parsers.ReplaceParser

class OTLDelta(sq: SimpleQuery) extends OTLBaseCommand(sq) with ReplaceParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("p")

  override def transform(_df: DataFrame): DataFrame = {
    val p = getKeyword("p").getOrElse(
      "1").toInt
    val win = Window.rowsBetween(-p, 0)
    returns.fields.foldLeft(_df) {
      case (accum, ReturnField(newfield, field)) =>
        val nf = if (newfield == field) s"delta($field)" else newfield
        accum.withColumn(nf, col(field)
          .over(win)
          .alias(nf))
          .withColumn(nf, col(field) - col(nf))
    }
  }
}
