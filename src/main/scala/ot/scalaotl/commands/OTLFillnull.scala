package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.functions.expr

import scala.util.{Success, Try}
import ot.scalaotl.extensions.StringExt._

/** =Abstract=
 * This class provides support of __'''fillnull'''__ otl command.
 *
 * __'''fillnull'''__ changes __NULL__ values of either all fields
 *
 * or specific fields to the value specified in the arguments.
 *
 * __'''fillnull'''__ takes two optional arguments:
 *
 *    1. __'''value'''__ - empty spaces (NULLs) in the field will be filled with this value,
 *    by default __'''value'''__ = 0
 *    1. the list of fields to be modified can be specified after the __'''fillnull'''__,
 *    by default __'''fillnull'''__ works with all fields
 *
 * =Usage example=
 * OTL:
 * {{{| makeresults | eval a = mvappend(1,2,3,NULL, NULL) | eval b = NULL | mvexpand a
 *| fillnull value=1 a,b}}}
 * Result:
 * {{{+----------+---+---+
|     _time|  b|  a|
+----------+---+---+
|1646048207|  1|1.0|
|1646048207|  1|2.0|
|1646048207|  1|3.0|
|1646048207|  1|1.0|
|1646048207|  1|1.0|
+----------+---+---+}}}
 *
 *
 * @constructor creates new instance of [[OTLFillnull]]
 * @param sq [[SimpleQuery]]
 */
class OTLFillnull(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("value")
  val searchId: Int = sq.searchId

  /**
   *
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return _df without __NULL__ values in all or some fields
   */
  override def transform(_df: DataFrame): DataFrame = {
    val fillValue = getKeyword("value").getOrElse("0")
    val schema = _df.schema
    val fields = if (returns.flatFields.isEmpty) _df.columns.toList else returns.flatFields
    fields.map(_.stripBackticks()).intersect(_df.columns)
      .foldLeft(_df) { (acc, name) =>
        val backtickedName = name.addSurroundedBackticks
        if (schema(name).dataType == StringType)
          acc.withColumn(name, expr(s"ifnull($backtickedName, $fillValue)"))
        else if (schema(name).dataType.isInstanceOf[NumericType]) {
          Try(fillValue.toDouble) match {
            case Success(_) => acc.withColumn(name, acc(backtickedName).cast(DoubleType)).withColumn(name, expr(s"ifnull($backtickedName, $fillValue)"))
            case _ => acc
          }
        }
        else acc.withColumn(name, acc(backtickedName).cast(StringType)).withColumn(name, expr(s"ifnull($backtickedName, $fillValue)"))
      }
  }
}
