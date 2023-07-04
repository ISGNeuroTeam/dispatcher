package ot.scalaotl.commands

import org.apache.spark.sql.DataFrame
import ot.AppConfig
import ot.scalaotl.SimpleQuery
import ot.scalaotl.extensions.StringExt._

class OTLCheckpoints(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  override val requiredKeywords =  Set.empty[String]
  override val optionalKeywords =  Set.empty[String]

  override def transform(_df: DataFrame): DataFrame = {
    val command = if(returns.flatFields.nonEmpty)
      returns.flatFields.head
    else
      throw new Exception
    if (command.stripBackticks() == "on" && !AppConfig.withCheckpoints)
      AppConfig.withCheckpoints = true
    else if (command.stripBackticks() == "off" && AppConfig.withCheckpoints)
      AppConfig.withCheckpoints = false
    _df
  }
}
