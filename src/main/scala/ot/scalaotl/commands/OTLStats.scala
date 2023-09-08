package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import ot.scalaotl.commands.commonconstructions.{StatsContext, StatsTransformer}
import ot.scalaotl.parsers.{StatsParser, WildcardParser}

class OTLStats(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) with StatsParser with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override val fieldsUsed: List[String] = getFieldsUsed(returns) ++ getPositionalFieldUsed(positionals)
  override val fieldsGenerated: List[String] = getFieldsGenerated(returns)

  override def transform(_df: DataFrame): DataFrame = {
    val statsTransformer = new StatsTransformer(Left(StatsContext(returns, positionalsMap, getKeyword("timeCol").getOrElse("_time"))), spark)
    statsTransformer.transform(_df)
  }

}
