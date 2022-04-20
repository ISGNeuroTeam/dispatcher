package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, split}
import ot.scalaotl.extensions.StringExt._

/** =Abstract=
 * This class provides support of __'''makemv'''__ otl command.
 *
 * __'''makemv'''__ converts a single-value field to a multi-value field,
 *
 * separating it with the specified string delimiter.
 *
 * __'''makemv'''__ takes on required and one optional argument:
 *  1. field to be converted
 *  1. __delim__ - string to be used as delimiter,
 *  by default __delim__ = " "
 *
 * =Usage example=
 * OTL:
 * {{{| makeresults count=2 | eval a = "2,3", b = "2,3" | makemv a delim=, }}}
 * Result:
 *{{{+----------+------+---+
|     _time|     a|  b|
+----------+------+---+
|1646135850|[2, 3]|2,3|
|1646135850|[2, 3]|2,3|
+----------+------+---+}}}
 * @constructor creates new instance of [[OTLMakemv]]
 * @param sq [[SimpleQuery]]
 */
class OTLMakemv(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("delim")

  /**
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   */
  override def transform(_df: DataFrame): DataFrame = {
    val delim = getKeyword("delim").getOrElse("\\s").strip("\"")
    returns.flatFields.headOption match {
      case Some(head) => _df.withColumn(head.strip("`"), split(col(head), delim))
      case None => _df
    }
  }
}
