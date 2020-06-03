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
  val cache = sq.cache
  val subsearches = sq.subsearches
  val argsSubsearch = getSubsearch(args)
  val ssid = getKeyword("subsearch").getOrElse(UUID.randomUUID().toString)
  val ssQuery = subsearches.getOrElse(ssid, argsSubsearch.getOrElse(""))
  val ssConv = new Converter(OTLQuery(ssQuery), cache)
  override val fieldsUsed = ssConv.fieldsUsed.toList
  override def transform(_df: DataFrame): DataFrame = {
    val ssDf = ssConv.setDF(_df).run
    _df.append(ssDf)
  }
}
