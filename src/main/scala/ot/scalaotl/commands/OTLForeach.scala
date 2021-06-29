package ot.scalaotl
package commands

import ot.dispatcher.OTLQuery
import ot.scalaotl.parsers.{ SubsearchParser, WildcardParser }
import ot.scalaotl.extensions.StringExt._

import org.apache.spark.sql.DataFrame

class OTLForeach(sq: SimpleQuery) extends OTLBaseCommand(sq) with SubsearchParser with WildcardParser {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("subsearch")
  val passedSubsearches: Map[String, String] = sq.subsearches
  val argsSubsearch: Option[String] = getSubsearch(args)
  
  override val fieldsUsed: List[String] = getFieldsUsed(returns)
    
  override def transform(_df: DataFrame): DataFrame = {
    val returnsWcFields: Return = returnsWithWc(_df.columns, returns)
    val matchstrMap: Map[String, String] = getMatches(_df.columns, returns).toMap
    val ssid = getKeyword("subsearch").getOrElse("__noPassedSubsearch__")
    
    // Try to get subsearch from array passed to class instance in SimpleQuery.
    // On failure, try to get subsearch from args.
    val subsearchQuery = passedSubsearches.getOrElse(
        ssid,
        argsSubsearch.getOrElse("")
    )
    returnsWcFields.flatFields.foldLeft(_df) {
      case (accum, item) =>
        // .stripBackticks is required because new Converter() add another pair of backticks
        val replacedQuery: String = subsearchQuery
          .replace("<<FIELD>>", item.stripBackticks())
          .replace("<<MATCHSTR>>", matchstrMap.getOrElse(item, ""))
        new Converter(OTLQuery(replacedQuery)).setDF(accum).run
    }
  }
}
