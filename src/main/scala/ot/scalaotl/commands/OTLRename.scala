package ot.scalaotl
package commands

import ot.scalaotl.parsers.{ ReplaceParser, WildcardParser }
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.extensions.DataFrameExt._

import org.apache.spark.sql.DataFrame

class OTLRename(sq: SimpleQuery) extends OTLBaseCommand(sq) with WildcardParser with ReplaceParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  override val fieldsGenerated: List[String] = returns.flatNewFields.diff(fieldsUsed)

  override def transform(_df: DataFrame): DataFrame = {
    returnsWithWc(_df.columns, returns).fields.foldLeft(_df) {
      case (accum, ReturnField(newfield, field)) =>
        accum.withSafeColumnRenamed(field.stripBackticks(), newfield.stripBackticks().strip("\""))
    }
  }
}
