package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame

class OTLRest(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("subsearch")
  val cachedDFs: Map[String, DataFrame] = sq.cache
  override def transform(_df: DataFrame): DataFrame = {
    val subsearch = getKeyword("subsearch").getOrElse("__nosubsearch__")
    cachedDFs.getOrElse(subsearch, _df)
  }
}
