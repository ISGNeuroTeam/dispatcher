package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ lit, when, col }

case class Range(lower: Double, upper: Double)
object Range {
  def apply(arr: Array[Double]): Range = arr.toList match {
    case first :: second :: tail => new Range(first, second)
    case _                       => new Range(0, Double.MaxValue)
  }
}

class OTLRangemap(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set("field")
  val optionalKeywords= Set.empty[String]

  override def fieldsGenerated = List("range")

  override def transform(_df: DataFrame): DataFrame = {
    val field = keywordsMap.get("field").map { case Keyword(k, v) => v }.getOrElse(return _df)
    if (_df.columns.contains(field)) {
      val kwMapDef = if (keywordsMap.contains("default")) keywordsMap else { keywordsMap + ("default" -> Keyword("default", "None")) }
      val default = kwMapDef.get("default").map { case Keyword(k, v) => v }.getOrElse("default")
      val valMap = (kwMapDef - "field" - "default").map { case (k, Keyword(_, v)) => k -> Range(v.split("-").map(_.toDouble)) } // TODO. Add NumberFormatException handling
      valMap.foldLeft(_df.withColumn("range", lit(default))) {
        case (accum, (name, range)) =>
          accum.withColumn("range", when((col(field) >= range.lower) && (col(field) < range.upper), lit(name)).otherwise(col("range")))
      }
    } else _df
  }
}
