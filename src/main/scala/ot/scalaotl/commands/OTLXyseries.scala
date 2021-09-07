package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.first

class OTLXyseries(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def transform(_df: DataFrame): DataFrame = {
    returns.flatFields match {
      case fixed :: groups :: values :: tail => _df.groupBy(fixed).pivot(groups).agg(first(values))
      case _                                 => _df
    }
  }
}
