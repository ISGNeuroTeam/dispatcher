package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import ot.scalaotl.commands.commonconstructions.{SortContext, SortTransformer}

class OTLSort(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def fieldsUsed: List[String] = returns.flatFields.map(_.replace("+", "").replace("-", ""))

  override def transform(_df: DataFrame): DataFrame = {
    val workTransformer = new SortTransformer(sq, Some(SortContext(args, returns.fields, keywordsMap)))
    workTransformer.transform(_df)
  }
}
