package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr, explode}
import ot.scalaotl.extensions.StringExt._

class OTLUntable(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def transform(_df: DataFrame): DataFrame = {
    _df.printSchema()
    val (fixed, field, value) = returns.flatFields match {
      case x :: y :: z :: tail => (
        x.stripBackticks().strip("\""),
        y.stripBackticks().strip("\""),
        z.stripBackticks().strip("\"")
      )
      case _ => return _df
    }
    val cols = _df.columns.filter(x => x != fixed && x != field && x != value).map(_.addSurroundedBackticks)
    cols.foldLeft(_df) {
      (accum, colname) => {
        accum.withColumn(colname.stripBackticks(), expr(s"""array("${colname.stripBackticks()}", $colname)"""))
      }
    }
      .withColumn("arr", expr(s"""array(${cols.mkString(", ")})"""))
      .select(fixed.strip("\""), "arr")
      .withColumn("arr", explode(col("arr")))
      .withColumn(field.strip("\""), col("arr").getItem(0))
      .withColumn(value.strip("\""), col("arr").getItem(1))
      .drop("arr")
  }
}
