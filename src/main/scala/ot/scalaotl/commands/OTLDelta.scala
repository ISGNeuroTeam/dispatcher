package ot.scalaotl
package commands

import ot.scalaotl.parsers.ReplaceParser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, first}
import org.apache.spark.sql.expressions.Window

/** =Abstract=
 * This class provides support of __'''delta'''__ otl command.
 *
 * __'''delta'''__ creates a field of differences between neighboring elements of another numeric field.
 *
 * __'''delta'''__ takes two required and one optional argument:
 *
 *  1. the name of the field for which the delta should be calculated, written after __'''delta'''__
 *  1. the name of the new field in which the results will be written, indicated after __as__
 *  1. __p__ argument - the difference is calculated by subtraction between the element numbered '''n''' and '''n-p''',
 * if there is no element n-p, then element n-p+1 is taken and so on, by default '''p''' = 1
 *
 * =Usage example=
 * OTL:
 * {{{| makeresults  | eval a = mvappend(1.5, 2, -30, 4, 0) | mvexpand a
 *| delta a p=2 as deltaA}}}
 *
 * Result:
 * {{{+----------+-----+------+
 *|     _time|    a|deltaA|
 *+----------+-----+------+
 *|1645784230|  1.5|   0.0|
 *|1645784230|  2.0|   0.5|
 *|1645784230|-30.0| -31.5|
 *|1645784230|  4.0|   2.0|
 *|1645784230|  0.0|  30.0|
 *+----------+-----+------+}}}
 *
 * @constructor creates new instance of [[OTLDelta]]
 * @param sq [[SimpleQuery]]
 */
class OTLDelta(sq: SimpleQuery) extends OTLBaseCommand(sq) with ReplaceParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("p")

  /**
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return _df with calculated delta field
   */
  override def transform(_df: DataFrame): DataFrame = {
    val p = getKeyword("p").getOrElse("1").toInt
    val win = Window.rowsBetween(-p, 0)
    returns.fields.foldLeft(_df) {
      case (accum, ReturnField(newfield, field)) =>
        val nf = if (newfield == field) s"delta($field)" else newfield
        accum.withColumn(nf, first(col(field)).over(win).alias(nf))
          .withColumn(nf, col(field) - col(nf))
    }
  }
}
