package ot.scalaotl
package commands

import ot.scalaotl.parsers.StatsParser
import ot.scalaotl.extensions.StringExt._

import org.apache.spark.sql.DataFrame

class OTLEventstats(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) with StatsParser {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set.empty[String]
  val transformer = new OTLStats(SimpleQuery(args))
  override val fieldsUsed = transformer.fieldsUsed
  override val fieldsGenerated = transformer.fieldsGenerated.map(_.stripBackticks())

  override def transform(_df: DataFrame): DataFrame = {
    val _df_out = transformer.transform(_df)    
    val res = positionalsMap.get("by") match {
      case Some(Positional("by", List())) => _df.drop(fieldsGenerated:_*).crossJoin(_df_out)
      case Some(Positional("by", byList)) => _df.drop(fieldsGenerated:_*).join(_df_out, byList.map(_.stripBackticks))
      case _                              => _df_out
    }
    res
  }
}
