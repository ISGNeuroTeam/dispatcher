package ot.scalaotl
package commands

import ot.scalaotl.parsers.ReplaceParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ col, regexp_replace }
import ot.scalaotl.extensions.StringExt._

case class ColumnNotFoundException(colname: String, columns: String)
  extends Exception(s"Column $colname does not exist in dataframe with columns $columns")

class OTLReplace(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("in")) with ReplaceParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def fieldsUsed: List[String] = getPositional("in").getOrElse(List[String]())
  override def transform(_df: DataFrame): DataFrame = {
    val colToReplace = fieldsUsed.headOption.getOrElse("")
    if (_df.columns.contains(colToReplace.stripBackticks())) {
      returns.fields.foldLeft(_df) {
        case (accum, ReturnField(replacement, rexStr)) =>
          val rex = rexStr.stripBackticks().stripPrefix("\"").stripSuffix("\"").replace("*", ".*")
          accum.withColumn(colToReplace.stripBackticks(), regexp_replace(col(colToReplace), rex, replacement.stripBackticks()))
      }
    } else {
      throw ColumnNotFoundException(colToReplace, s"[${_df.columns.mkString(", ")}]")
    }
  }
}
