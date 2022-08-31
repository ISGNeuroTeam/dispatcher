package ot.scalaotl.commands

import org.apache.spark.sql.DataFrame
import ot.scalaotl.SimpleQuery

class OTLComment(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  override val requiredKeywords: Set[String] = Set.empty[String]
  override val optionalKeywords: Set[String] = Set.empty[String]
  override val fieldsUsed = List.empty[String]

  override def transform(_df: DataFrame): DataFrame = _df
}
