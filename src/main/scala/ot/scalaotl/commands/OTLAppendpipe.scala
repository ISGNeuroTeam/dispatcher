package ot.scalaotl
package commands

import java.util.UUID

import ot.dispatcher.OTLQuery
import org.apache.spark.sql.DataFrame
import ot.scalaotl.parsers.SubsearchParser
import ot.scalaotl.extensions.DataFrameExt._

class OTLAppendpipe(sq: SimpleQuery) extends OTLBaseCommand(sq) with SubsearchParser {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("subsearch")
  val cache: Map[String, DataFrame] = sq.cache
  val subsearches: Map[String, String] = sq.subsearches
  val argsSubsearch: Option[String] = getSubsearch(args)
  val ssid: String = getKeyword("subsearch").getOrElse(UUID.randomUUID().toString)
  val ssQuery: String = subsearches.getOrElse(ssid, argsSubsearch.getOrElse(""))
  val ssConv = new Converter(OTLQuery(ssQuery), cache)
  override val fieldsUsed: List[String] = ssConv.fieldsUsed.toList
  override def transform(_df: DataFrame): DataFrame = {
    val ssDf = ssConv.setDF(_df).run
    _df.append(ssDf)
  }
}
