package ot.scalaotl
package commands

import ot.scalaotl.parsers.{ RenameParser, WildcardParser }
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.extensions.DataFrameExt._

import org.apache.spark.sql.DataFrame

class OTLRename(sq: SimpleQuery) extends OTLBaseCommand(sq) with WildcardParser with RenameParser {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set.empty[String]
  override val fieldsGenerated = returns.flatNewFields.diff(fieldsUsed)

  override def transform(_df: DataFrame): DataFrame = {
    returnsWithWc(_df.columns, returns).fields.foldLeft(_df) {
      case (accum, ReturnField(newfield, field)) =>
        val fld = field.stripBackticks
        val nfld = newfield
        accum.withSafeColumnRenamed(fld, nfld)
    }
  }
}
