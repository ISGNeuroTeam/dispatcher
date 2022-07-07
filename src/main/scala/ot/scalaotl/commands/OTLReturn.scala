package ot.scalaotl
package commands

import scala.util.Try
import org.apache.spark.sql.DataFrame
import ot.scalaotl.extensions.StringExt._
import org.apache.spark.sql.functions._

/** =Abstract=
 * This class provides support of __'''return'''__ otl command.
 *
 * __'''return'''__ used to keeps or remove the selected fields from the query results.
 * If the command is placed at the end of the request, then the indicated fields will be displayed on the screen.
 * If the command is specified in the middle of the query, then further data processing will occur with the fields
 * specified after fields.
 * Fields is a synonym for the command table.
 *
 * Command syntax return [count] <field-list>
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
 * @constructor creates new instance of [[ OTLReturn ]]
 * @param sq [[ SimpleQuery ]] - contains args, cache, subsearches, search time interval, stfe and preview flags
 */
class OTLReturn(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def fieldsUsed: List[String] = getFieldsUsed(returns).map(_.stripBackticks().stripPrefix("$").addSurroundedBackticks)

  def isInteger(s: String): Boolean = Try(s.toInt).isSuccess

  val count: Int = args.split(" ").headOption.filter(isInteger) match {
    case Some(str) => str.toInt
    case _ => 1
  }

  val fields: List[String] = returns.flatFields.filter(_ != count.toString.addSurroundedBackticks)

  /**
   * Standard method called by Converter in each OTL-command.
   *
   * @param _df [[DataFrame]] - incoming dataset (in generator-command like this one is ignored and should be empty)
   * @return [[DataFrame]]  - outgoing dataframe with OTL-command results
   */
  override def transform(_df: DataFrame): DataFrame = {
    val nFields = fields.map(_.stripBackticks()).filter(_.startsWith("$"))
    val ndf = nFields.foldLeft(_df)((acc, f) => acc.withColumn(f, col(f.stripPrefix("$"))))
    val dfLimit = ndf.limit(count).select(fields.head, fields.tail: _*)
    dfLimit
  }
}
