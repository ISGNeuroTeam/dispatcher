package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.StatsParser

class OTLEventstats(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) with StatsParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  val transformer = new OTLStats(SimpleQuery(args))
  override val fieldsUsed: List[String] = transformer.fieldsUsed
  override val fieldsGenerated: List[String] = transformer.fieldsGenerated.map(_.stripBackticks())

  override def transform(_df: DataFrame): DataFrame = {
    positionalsMap.get("by") match {
      case Some(Positional("by", anyList)) if anyList.intersect(_df.columns).isEmpty => spark.emptyDataFrame
      case pos =>
        val _df_out = transformer.transform(_df)
        pos match {
          case Some(Positional("by", byList)) =>
            byList match {
              case List() => _df.drop(fieldsGenerated: _*).crossJoin(_df_out)
              case _ => _df.drop(fieldsGenerated: _*).join(_df_out, byList.map(_.stripBackticks()))
            }
          case _ => _df_out
        }
    }
  }
}
