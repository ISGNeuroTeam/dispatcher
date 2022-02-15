package ot.scalaotl
package commands

import ot.scalaotl.extensions.StringExt._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, desc}

class OTLSort(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def fieldsUsed: List[String] = returns.flatFields.map(_.replace("+", "").replace("-", ""))

  val order: Array[String] = "\\s+".r.replaceAllIn(
    "[-+,']".r.replaceAllIn(args, ""),
    " "
  )
    .split(" ")
  // .map(_.strip("'"))

  override def transform(_df: DataFrame): DataFrame = {
    def sortBeautifier(s: String) = s.strip("'").addSurroundedBackticks

    val sortCols = returns.fields
      .map(x => x.field.stripBackticks())
      .map {
        field =>
          if (field.startsWith("-")) (sortBeautifier(field.drop(1)), "-")
          else if (field.startsWith("+")) (sortBeautifier(field.drop(1)), "+") else (sortBeautifier(field), "+")
      }.toMap
    val sortList = order.intersect(_df.columns.toList)
      .map(x => x.addSurroundedBackticks)
      .map { x => if (sortCols(x) == "+") asc(x) else desc(x) }
      .toList

    keywordsMap.get("count") match {
      case Some(Keyword("count", value)) => _df.sort(sortList: _*).limit(value.toInt)
      case _ => _df.sort(sortList: _*)
    }
  }
}
