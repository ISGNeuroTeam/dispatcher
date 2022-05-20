package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}
import ot.scalaotl.extensions.StringExt._

/** =Abstract=
 * This class provides support of __'''nomv'''__ otl command.
 *
 * __'''nomv'''__ converts multivalue field values to single values.
 *
 * __'''nomv'''__ takes one required argument -
 *
 * the name of the field whose values need to be changed.
 *
 * =Usage example=
 * OTL 1:
 * {{{makeresults | eval a = mvappend("hello ","world")}}}
 * Result 1:
 * {{{+----------+---------------+
|     _time|              a|
+----------+---------------+
|1646238260|[hello , world]|
+----------+---------------+}}}
 * OTL 2:
 * {{{| makeresults | eval a = mvappend("hello ","world") | nomv a}}}
 * Result 2:
 * {{{+----------+------------+
|     _time|           a|
+----------+------------+
|1646238260|hello  world|
+----------+------------+}}}
 *
 * @constructor creates new instance of [[OTLNomv]]
 * @param sq [[SimpleQuery]]
 */
class OTLNomv(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  /**
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   */
  override def transform(_df: DataFrame): DataFrame = {
    val result = returns.flatFields.headOption match {
      case Some(field) => _df.withColumn(field.stripBackticks(), concat_ws(" ", col(field)))
      case _ => _df
    }
    result
  }
}
