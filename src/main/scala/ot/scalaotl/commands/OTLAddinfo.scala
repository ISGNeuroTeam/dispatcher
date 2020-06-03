package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class OTLAddinfo(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set.empty[String]
  val id = sq.searchId
  val tws = sq.tws
  val twf = sq.twf

  override def fieldsGenerated = List("info_min_time", "info_max_time", "info_sid")

  override def transform(_df: DataFrame): DataFrame = {
    _df.withColumn("info_min_time", lit(tws))
      .withColumn("info_max_time", lit(twf))
      .withColumn("info_sid", lit(id))
  }
}
