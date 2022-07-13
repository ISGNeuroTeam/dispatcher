package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.collect_list
import ot.scalaotl.extensions.StringExt._

/** =Abstract=
 * This class provides support of __'''mvcombine'''__ otl command.
 *
 * __'''mvcombine'''__ combines a group of events that differ by one field into one event.
 *
 * The values of the field on which the combination is made
 *
 * are merged into an array of values.
 *
 * __'''mvcombine'''__ takes one required argument-
 *
 * the field on which the combination is performed.
 *
 * =Useage example=
 * OTL 1:
 * {{{| makeresults | eval a = mvappend(1,2,1,1,3), b = 2 | mvexpand a}}}
 * Result 1:
 * {{{+----------+---+---+
|     _time|  b|  a|
+----------+---+---+
|1646231725|  2|  1|
|1646231725|  2|  2|
|1646231725|  2|  1|
|1646231725|  2|  1|
|1646231725|  2|  3|
+----------+---+---+}}}
 * OTL 2:
 * {{{| makeresults | eval a = mvappend(1,2,1,1,3), b = 2 | mvexpand a
 *| mvcombine a}}}
 * Result 2:
 * {{{+----------+---+---------------+
|     _time|  b|              a|
+----------+---+---------------+
|1646231725|  2|[1, 2, 1, 1, 3]|
+----------+---+---------------+}}}
 *
 *
 * @constructor creates new instance of [[OTLMvcombine]]
 * @param sq [[SimpleQuery]]
 */
class OTLMvcombine(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  /**
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return _df with events combined by specified field
   */
  override def transform(_df: DataFrame): DataFrame = {
    val field = returns.flatFields.headOption.getOrElse(return _df)
    val groupCols = _df.columns.filter(_ != field.stripBackticks()).toList
    groupCols match {
      case head :: tail => _df.groupBy(head, tail: _*).agg(collect_list(field).alias(field.stripBackticks()))
      case _ => _df
    }
  }
}
