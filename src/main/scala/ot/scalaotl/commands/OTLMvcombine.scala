package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.collect_list
import ot.scalaotl.extensions.StringExt._

class OTLMvcombine(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set.empty[String]
  override def transform(_df: DataFrame): DataFrame = {
    val field = returns.flatFields.headOption.getOrElse(return _df)
    val groupCols = _df.columns.filter(_ != field.stripBackticks()).toList
    groupCols match {
      case head :: tail => _df.groupBy(head, tail: _*).agg(collect_list(field).alias(field.stripBackticks()))
      case _            => _df
    }
  }
}
