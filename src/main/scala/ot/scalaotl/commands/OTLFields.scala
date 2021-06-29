package ot.scalaotl
package commands

import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.WildcardParser

import org.apache.spark.sql.DataFrame

class OTLFields(sq: SimpleQuery) extends OTLBaseCommand(sq) with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  override val fieldsUsed: List[String] = if (returns.flatFields.contains("`-`")) List() else returns.flatFields.map(x=> x.strip("`").strip("\"").addSurroundedBackticks)


  override def transform(_df: DataFrame): DataFrame = {
    val initCols = _df.columns.map(_.addSurroundedBackticks)
    val retCols = returns.flatFields.map(_.replace("\"", ""))
    val retColsWc = returnsWithWc(initCols, returns).flatFields.map(_.replace("\"", "")).union(retCols)
    //It is correct to use retCols in 'if' statement (not retColsWc) because applying wildcards removes "-" usually
    val initColsList = initCols.toList
    val newCols = if (retCols.contains("`-`")) {
      initColsList.diff(retColsWc)
    } else {
      retColsWc.intersect(initColsList)
    }
    newCols match {
      case head :: tail => _df.select(head, tail: _*)
      case _            => _df
    }
  }
}
