package ot.scalaotl
package commands

import com.isgneuro.otl.processors.Rename
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.CustomException.E00012
import ot.scalaotl.extensions.StringExt.BetterString
import ot.scalaotl.parsers.{RenameParser, WildcardParser}

class OTLRename(sq: SimpleQuery) extends OTLBaseCommand(sq) with WildcardParser with RenameParser {

  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  override val fieldsGenerated: List[String] = returns.flatNewFields.diff(fieldsUsed)

  override def transform(_df: DataFrame): DataFrame = {
    val existingFieldNames = returns.fields.map(f => f.field.stripBackticks())
    val newFieldNames = returns.fields.map(f => f.newfield)
    if (existingFieldNames.length != newFieldNames.length || existingFieldNames.isEmpty) {
      throw E00012(sq.searchId, "rename", "wc-field")
    }
    val worker = new Rename(returns.fields.map(f => f.field.stripBackticks()), returns.fields.map(f => f.newfield))
    worker.transform(_df)
  }
}
