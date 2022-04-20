package ot.scalaotl
package commands

import ot.scalaotl.extensions.StringExt._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{sum, expr, when, col}

/** =Abstract=
 * This class provides support of __'''head'''__ otl command.
 *
 * __'''head'''__ returns the first '''N''' rows of the input dataset.
 *
 * __'''head'''__ takes one optional argument - '''__limit__''',
 *
 * by default '''__limit__''' = 10.
 *
 * '''__limit__''' also can be set without keyword, by number after __'''head'''__.
 *
 * '''__IMPORTANT__''': the same order of events cannot be guaranteed
 *
 * for different query runs. To preserve the order of events, you need to sort
 *
 * the events before __'''head'''__.
 *
 * =Usage example=
 * ==Example 1==
 * OTL:
 * {{{| makeresults | eval  a = mvappend(1,2,3,4,5) | mvexpand a | fields a | head 3}}}
 * Result:
 *{{{+---+
|  a|
+---+
|  1|
|  2|
|  3|
+---+}}}
 * ==Example 2==
 * OTL:
 * {{{| makeresults | eval  a = mvappend(1,2,3,4,5) | mvexpand a | fields a | head limit=2}}}
 * Result:
 *{{{+---+
|  a|
+---+
|  1|
|  2|
+---+}}}
 *
 * @constructor creates new instance of [[OTLHead]]
 * @param sq [[SimpleQuery]]
 */
class OTLHead(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("limit")

  override def fieldsUsed: List[String] = List()

  /**
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return limited _df
   */
  override def transform(_df: DataFrame): DataFrame = {
    def isAllDigits(x: String) = x.forall(Character.isDigit)

    val lim = getKeyword("limit").getOrElse("10").toInt
    if (returns.flatFields.isEmpty) {
      _df.limit(lim)
    } else {
      val expression = returns.flatFields.mkString(" ").stripBackticks()
      if (isAllDigits(expression)) {
        _df.limit(expression.toInt)
      } else {
        val ws = Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        _df.withColumn("cnt", sum(when(expr(expression), 0).otherwise(1)).over(ws))
          .filter(col("cnt") === 0)
          .drop("cnt")
          .limit(lim)
      }
    }
  }
}
