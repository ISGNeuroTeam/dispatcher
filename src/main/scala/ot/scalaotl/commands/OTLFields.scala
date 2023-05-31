package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.WildcardParser


/** =Abstract=
 * This class provides support of __'''fields'''__ otl command.
 *
 * __'''fields'''__ used to keeps or remove the selected fields from the query results.
 * If the command is placed at the end of the request, then the indicated fields will be displayed on the screen.
 * If the command is specified in the middle of the query, then further data processing will occur with the fields
 * specified after fields.
 * Fields is a synonym for the command table.
 *
 * Command syntax fields [-|*] <field-list>
 *
 * If "-" is specified, then the fields from <field-list> will be removed from the query results.
 * If an asterisk "*" is specified, then all fields will be retrieved.
 * Field names can use wildcards.
 *
 * =Usage example=
 * OTL: keep only selected fields
 * {{{  other otl-commands ... | fields metric_name, value }}}
 *
 * OTL: remove selected fields
 * {{{  other otl-commands ... | fields - metric_long_name, _raw }}}
 *
 * OTL: keep selected fields with wildcards
 * {{{  other otl-commands ... | fields _*, metric*, value }}}
 *
 * OTL: get all fields from index
 * {{{  other otl-commands ... | fields * }}}
 *
 * @constructor creates new instance of [[ OTLFields ]]
 * @param sq [[ SimpleQuery ]] - contains args, cache, subsearches, search time interval, stfe and preview flags
 */
class OTLFields(sq: SimpleQuery) extends OTLBaseCommand(sq) with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override val fieldsUsed: List[String] =
    if (returns.flatFields.contains("`-`")) List()
    else returns.flatFields.map(x => x.strip("`").strip("\"").addSurroundedBackticks)


  val act: String = args.split(" ").headOption.filter(x => x.equals("+") || x.equals("-")) match {
    case Some(str) => str.substring(0, 1).addSurroundedBackticks
    case _ => "+".addSurroundedBackticks
  }

  /**
   * Standard method called by Converter in each OTL-command.
   *
   * @param _df [[DataFrame]] - incoming dataset (in generator-command like this one is ignored and should be empty)
   * @return [[DataFrame]]  - outgoing dataframe with OTL-command results
   */
  override def transform(_df: DataFrame): DataFrame = {
    val initCols = _df.columns.map(_.addSurroundedBackticks)
    // field-list from the query
    val retCols = returns.flatFields
      .filter(x => !x.equals(act))
      .map(_.replace("\"", ""))
    // list of fields from the df matching regular expressions in the field-list from the query
    val retColsWc = returnsWithWc(initCols, returns).flatFields
      .filter(x => !x.equals(act))
      .map(_.replace("\"", "")).union(retCols)
    // it is correct to use retCols in 'if' statement (not retColsWc) because applying wildcards removes "-" usually
    val initColsList = initCols.toList
    // if field-list contains minus return difference from retColsWc, else intersection with retColsWc
    val newCols = if (act.equals("`-`")) {
      initColsList.diff(retColsWc)
    } else {
      retColsWc.intersect(initColsList)
    }
    // if newCols is empty return original df, otherwise return the selected columns from it
    newCols match {
      case head :: tail => _df.select(head, tail: _*)
      case _ => spark.emptyDataFrame
    }
  }
}
