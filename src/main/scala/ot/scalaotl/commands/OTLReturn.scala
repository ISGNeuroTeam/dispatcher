package ot.scalaotl
package commands

import scala.util.Try
import org.apache.spark.sql.DataFrame
import ot.scalaotl.extensions.StringExt._
import org.apache.spark.sql.functions._

/** =Abstract=
 * This class provides support of __'''return'''__ otl command.
 *
 * __'''return'''__ used in subqueries to substitute a value from a specific column of the received dataset
 * into the original query.
 * By default if more than one record is received during the subsearch evaluation, the command will return
 * only the first value from the specified column.
 * You can return multiple values by specifying the number of required values as the first parameter.
 *
 * Command syntax return [count] $fieldname
 *
 * The default count is 1.
 *
 * =Usage example=
 * OTL: return first value of field in subsearch
 * {{{  main search otl-commands ... eval x = [ subsearch otl-commands ... | return $fieldname }}}
 *
 * OTL: return multiple values of field in subsearch
 * {{{  main search otl-commands ... eval x = [ subsearch otl-commands ... | return count $fieldname }}}
 *
 * @constructor creates new instance of [[ OTLReturn ]]
 * @param sq [[ SimpleQuery ]] - contains args, cache, subsearches, search time interval, stfe and preview flags
 */
class OTLReturn(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def fieldsUsed: List[String] = getFieldsUsed(returns).map(_.stripBackticks().stripPrefix("$").addSurroundedBackticks)

  def isInteger(s: String): Boolean = Try(s.toInt).isSuccess

  // getting the number of return values
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
