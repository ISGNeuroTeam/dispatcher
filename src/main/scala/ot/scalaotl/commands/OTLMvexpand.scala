
package ot.scalaotl
package commands

import org.apache.spark.sql.functions.{col, explode, slice}
import org.apache.spark.sql.DataFrame
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.extensions.DataFrameExt._

/** =Abstract=
 * This class provides support of __'''mvexpand'''__ otl command.
 *
 * __'''mvexpand'''__ splits an event with a multivalue into several events.
 *
 * __'''mvexpand'''__ takes one required argument and one optional argument:
 *  1. multivalued field to be split
 *  1. __limit__ - determines how many values from the multivalue field
 *     will be used to create new events,
 *     by default all values from multivalue field are used.
 *
 * =Usage example=
 * ==Example 1==
 * OTL 1:
 * {{{| makeresults | eval a = mvappend(1,2,3,4,5)}}}
 * Result 1:
 * {{{+----------+---------------+
 *|     _time|              a|
 *+----------+---------------+
 *|1646233453|[1, 2, 3, 4, 5]|
 *+----------+---------------+}}}
 * OTL 2:
 * {{{| makeresults | eval a = mvappend(1,2,3,4,5) | mvexpand a}}}
 * Result 2:
 * {{{+----------+---+
 *|     _time|  a|
 *+----------+---+
 *|1646233453|  1|
 *|1646233453|  2|
 *|1646233453|  3|
 *|1646233453|  4|
 *|1646233453|  5|
 *+----------+---+}}}
 *
 * ==Example 2==
 * OTL:
 * {{{| makeresults | eval a = mvappend(1,2,3,4,5)
 *| mvexpand a limit=3 }}}
 * Result:
 * {{{+----------+---+
 *|     _time|  a|
 *+----------+---+
 *|1646233747|  1|
 *|1646233747|  2|
 *|1646233747|  3|
 *+----------+---+}}}
 *
 * @constructor creates new instance of [[OTLMvexpand]]
 * @param sq [[SimpleQuery]]
 */
class OTLMvexpand(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("limit")

  /** Splits a field containing a multivalue using the slice [[org.apache.spark.sql.functions.slice()]]
   * and explode [[org.apache.spark.sql.functions.explode()]].
   *
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   */
  override def transform(_df: DataFrame): DataFrame = {
    val limit = getKeyword("limit").getOrElse("0").toInt
    returns.flatFields match {
      case field :: tail =>
        val colToExplode = if (limit > 0) slice(col(field), 1, limit) else col(field)
        val fieldNoBcktck = field.stripBackticks()
        val fieldType = _df.schema.toList.collectFirst { case x if x.name == fieldNoBcktck => x.dataType.typeName }.getOrElse("")
        val f = fieldType match {
          case "array" => explode(colToExplode)
          case _ => colToExplode
        }
        _df.withColumn("exploded_" + fieldNoBcktck, f)
          .drop(fieldNoBcktck)
          .withSafeColumnRenamed("exploded_" + fieldNoBcktck, fieldNoBcktck)
      case _ => _df
    }
  }
}
