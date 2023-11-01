package ot.scalaotl
package commands

import com.isgneuro.otl.processors
import org.apache.spark.sql.DataFrame
import org.json4s._
import org.json4s.native.JsonMethods._
import ot.dispatcher.sdk.core.CustomException.E00026
import ot.scalaotl.config.OTLIndexes
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.{ExpressionParser, WildcardParser}

/** =Abstract=
 * This class provides support of __'''search'''__ otl command.
 *
 * __'''search'''__ used for read raw data from indexes (only _raw and _time fields)
 * Field _raw is used for search-time-field-extraction
 * The command can search for given words inside the _raw field
 * Search words can be combined via logical operations AND and OR
 * Logical operations names (AND and OR) are not case sensitive
 * The command can read data from multiple indexes (in this case indexes will be combined with logical OR)
 * Index names and search words can use wildcards
 *
 * =Usage example=
 * OTL: one index and one search word
 * {{{ search index="index_name" "search_word" | ... other otl-commands }}}
 *
 * OTL: two indexes and one search word
 * {{{ search index="first_index_name" index="second_index_name" "first_search_word" | ... other otl-commands }}}
 *
 * OTL: one index and two search words
 * {{{ search index="index_name" "first_search_word" AND "second_search_word" | ... other otl-commands }}}
 *
 * Also search command can be used in subqueries to filter the results of other queries
 * {{{ other otl-commands ... | search metric=temp* AND level=WARN* }}}
 *
 * @constructor creates new instance of [[ RawRead ]]
 * @param sq [[ SimpleQuery ]] - contains args, cache, subsearches, search time interval, stfe and preview flags
 */
class RawRead(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLIndexes with ExpressionParser with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords: Set[String] = Set("limit", "subsearch")
  val SimpleQuery(_args, searchId, cache, subsearches, tws, twf, stfe, preview) = sq
  // Command has no optional keywords, nothing to validate
  override def validateOptionalKeywords(): Unit = ()

  def jsonStrToMap(jsonStr: String): Map[String, Map[String, String]] = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Map[String, String]]]
  }

  // Search in the query for all indexes containing * and look for similar ones among the available indexes
  var indexQueriesMap: Map[String, Map[String, String]] = jsonStrToMap(excludeKeywords(_args.trim, List(Keyword("limit", "t"))))

  // Get a list of fields used in query (this list next used for build fieldsUsedInFullQuery)
  // Select query from each entry in indexQueriesMap
  // For each entry find in query substrings which not starting with ! or ( (may be counted values and inversion)
  // and containing substrings of the form: field name =|>|<|like|rlike value
  // For all found field names do strip ! and ' and \"
  // Then strip backtics and add surrounded backticks
  override val fieldsUsed: List[String] = indexQueriesMap.map {
    case (_, singleIndexMap) => singleIndexMap.getOrElse("query", "").withKeepQuotedText[List[String]](
      (s: String) => """(?![!(])(\S*?)\s*(=|>|<|like|rlike)\s*""".r.findAllIn(s).matchData.map(_.group(1)).toList
    )
      .map(_.strip("!").strip("'").strip("\"").stripBackticks().addSurroundedBackticks)
  }.toList.flatten

  /**
   * Standard method called by Converter in each OTL-command.
   *
   * @param _df [[DataFrame]] - incoming dataset (in generator-command like this one is ignored and should be empty)
   * @return [[DataFrame]]  - outgoing dataframe with OTL-command results
   */
  override def transform(_df: DataFrame): DataFrame = {
    var limit: Int = 0
    try {
      val limitText: String = getKeyword("limit") match {
        case Some(lim) => lim
        case _ => limit.toString
      }
      limit = limitText.toInt
    } catch {
      case e: IllegalArgumentException => throw E00026(searchId, e.getMessage)
    }
    val reader = new processors.RawRead(spark, log, searchId.toString, otlconfig, indexQueriesMap, limit, fieldsUsedInFullQuery)
    reader.transform(_df)
  }
}