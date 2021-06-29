package ot.scalaotl
package commands

import ot.scalaotl.extensions.StringExt._

import org.apache.spark.sql.{ DataFrame, Column }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ col, lit, concat, lag, monotonically_increasing_id, when }

class OTLDedup(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("sortby")) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set.empty[String]
  override def transform(_df: DataFrame): DataFrame = {
    val dfDedup = keywordsMap.get("consecutive") match {
      case Some(Keyword("consecutive", "t")) =>
        val expr = returns.flatFields.foldLeft(List[Column]()) { (accum, item) => col(item) :: (lit("#") :: accum) }
        _df.withColumn("idx", monotonically_increasing_id())
          .withColumn("dedupcol", concat(expr: _*))
          .withColumn("prevdedup", lag("dedupcol", 1).over(Window.orderBy("idx")))
          .filter(when(col("dedupcol") === col("prevdedup"), false).otherwise(true))
          .drop("dedupcol", "prevdedup", "idx")
      case _ => _df.dropDuplicates(returns.flatFields.map(_.stripBackticks()))
    }

    positionalsMap.get("sortby") match {
      case Some(Positional("sortby", sf)) => new OTLSort(SimpleQuery(sf.map(_.stripBackticks()).mkString(" "))).transform(dfDedup)
      case _                              => dfDedup
    }
  }
}
