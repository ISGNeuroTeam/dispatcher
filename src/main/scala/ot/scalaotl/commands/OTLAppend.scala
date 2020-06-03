package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import ot.scalaotl.extensions.DataFrameExt._

class OTLAppend(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("subsearch")
  val cache = sq.cache
  override def transform(_df: DataFrame): DataFrame = {
    val subsearch: String = getKeyword("subsearch").getOrElse("__nosubsearch__")
    val jdf: DataFrame = cache.getOrElse(subsearch, return _df)
    _df.append(jdf)
  }
}
