package ot.scalaotl.commands.commonconstructions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, desc}
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.DefaultParser
import ot.scalaotl.{Field, Keyword, ReturnField, SimpleQuery}

/**
 * Class, containing transforming function for structured data sorting.
 *  @param sq instance of sorting query
 *  @param context context class with fields, need for sorting work
 */
class SortTransformer(sq: SimpleQuery, context: Option[SortContext] = None) extends DefaultParser{
  val (args: String, returnFields: List[ReturnField], keywordsMap: Map[String, Field]) = context match {
    case Some(SortContext(a, r, k)) => (a, r, k)
    case _ => (sq.args, returnsParser(sq.args, Set.empty).fields, fieldsToMap(keywordsParser(sq.args)))
  }

  /**
   * @param _df input dataframe
   * @return _df, sorted by field, specified in query
   */
  def transform(_df: DataFrame): DataFrame = {
    val order: Array[String] = "\\s+".r.replaceAllIn(
      "[-+,']".r.replaceAllIn(args, ""),
      " "
    )
      .split(" ")
    def sortBeautifier(s: String) = s.strip("'").addSurroundedBackticks
    val sortCols = returnFields
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

case class SortContext(args: String, returnFields: List[ReturnField], keywordsMap: Map[String, Field])

