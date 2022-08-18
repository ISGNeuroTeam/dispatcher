package ot.scalaotl
package commands

import org.apache.spark.sql.functions.{concat_ws, col}
import org.apache.spark.sql.DataFrame

class OTLNomv(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def transform(_df: DataFrame): DataFrame = {
    returns.flatFields.headOption match {
      case Some(field) => _df.withColumn(field, concat_ws(" ", col(field)))
      case _ => _df
    }
  }
}
