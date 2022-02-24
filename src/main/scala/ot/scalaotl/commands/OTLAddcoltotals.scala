package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.NumericType

/** =Abstract=
 * This class provides support of __'''addcoltotals'''__ otl command.
 *
 * __'''addcoltotals'''__ adds the sum of each numeric field to the end of the set.
 *
 * If the __labelfield__ argument is specified,
 * column with the specified name is added to the result table.
 *
 * __label__ argument used with the __labelfield__ argument to add a label to the final event.
 *
 * If the __labelfield__ argument is missing, the __label__ argument has no effect.
 *
 * =Usage example=
 * OTL:
 * {{{| makeresults count = 5 | eval a = 1 | eval b = 2
 *| addcoltotals labelfield=total label=CustomLabel | fields a,b,total}}}
 *
 * Result:
 *{{{+---+---+-----------+
 *|  a|  b|      total|
 *+---+---+-----------+
 *|  1|  2|       null|
 *|  1|  2|       null|
 *|  1|  2|       null|
 *|  1|  2|       null|
 *|  1|  2|       null|
 *|  5| 10|CustomLabel|
 *+---+---+-----------+}}}
 *
 * @constructor creates new instance of [[OTLAddcoltotals]]
 * @param sq [[SimpleQuery]]
 */
class OTLAddcoltotals(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("labelfield", "label")

  /**
   *Tries to get the __label__ and __labelfield__ keywords,
   * if they are not specified, then they are taken by default as __labelfield__=None, __label__=Total
   *
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return _df with the sum of each numeric field added to the end
   */
  override def transform(_df: DataFrame): DataFrame = {

    val labelfield = getKeyword("labelfield").getOrElse("None")
    val label = getKeyword("label").getOrElse("Total")
    val fieldlist = if (returns.flatFields.isEmpty) _df.columns else returns.flatFields.toArray
    val numericList = _df.schema.filter(_.dataType.isInstanceOf[NumericType])

    if (numericList.isEmpty) {
      _df
    } else {
      val numeric = numericList.map(_.name)
        .toArray
        .filter(fieldlist.contains(_))
      val other = _df.columns.diff(numeric)
      val aggs = numeric.map(name => sum(col(name)).as(name))

      if (labelfield == "None") {
        _df.unionByName(other.foldLeft(_df.agg(aggs.head, aggs.tail: _*))((data, colName) => {
          data.withColumn(colName, lit(null))
        }))
      } else {
        val labeledDf = _df.withColumn(labelfield, lit(null))

        labeledDf.unionByName(other.foldLeft(labeledDf.agg(aggs.head, aggs.tail: _*))((data, colName) => {
          data.withColumn(colName, lit(null))
        }).withColumn(labelfield, lit(label)))
      }
    }
  }
}
