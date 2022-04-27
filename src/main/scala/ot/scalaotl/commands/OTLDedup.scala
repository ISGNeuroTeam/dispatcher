package ot.scalaotl
package commands

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import ot.scalaotl.extensions.StringExt._

/** =Abstract=
 * This class provides support of __'''dedup'''__ otl command.
 *
 * __'''dedup'''__ delete events that contain a duplicate field value or a duplicate identical combination of field values
 *                 specified as command parameters
 *
 * __'''dedup'''__ takes 1 required and 2 optional argument:
 *
 *    1.  _'''field-list'''_ - field or list of fields on which deduplication is performed
 *    2. _'''consecutive'''_ - parameter, the value of which is "true", sequential deduplication is performed -
 *      only duplicates in adjacent rows are deleted.
 *      A value of this parameter that is not equal to "true" is equivalent to the absence of the parameter.
 *    3. __'''sortby'''__ - a field for sorting the deduplication result in ascending order
 *      (if the '+' prefix is specified) or descending (if the '-' prefix is specified).
 *
 * =Usage examples=
 * OTL 1:
 * {{{| makeresults | eval a = mvappend(10,20,30),b=1 | append [makeresults | eval a = mvappend(90,70,80),b=2]
 * | mvexpand a | dedup b}}}
 * Result:
 * {{{+----------+---+---+
|     _time|  b|  a|
+----------+---+---+
|1650352465 | 2|90|
|1650352466	| 1|10|
+----------+---+---+}}}
 * OTL 2:
 * {{{| makeresults | eval a = mvappend(10,20,20),b=1 | append [makeresults | eval a = mvappend(90,90,80),b=2]
 * | mvexpand a | dedup a consecutive=true}}}
 * Result:
 * {{{+----------+---+---+
|     _time|  b|  a|
+----------+---+---+
|1650352069	| 1|10|
|1650352048	| 2|90|
|1650352069	| 1|20|
|1650352048	| 2|80|
+----------+---+---+}}}
 * OTL 3:
 * {{{| makeresults | eval a = mvappend(10,20,20),b=1 | append [makeresults | eval a = mvappend(90,90,80),b=2]
 * | mvexpand a | dedup b sortby a}}}
 * Result:
 * {{{+----------+---+---+
|     _time|  b|  a|
+----------+---+---+
|1650352636	| 1|10|
|1650352636	| 2|90|
+----------+---+---+}}}
 * @constructor creates new instance of [[OTLDedup]]
 * @param sq [[SimpleQuery]]
 */
class OTLDedup(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("sortby")) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  /**
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return _df with events combined by specified field
   */
  override def transform(_df: DataFrame): DataFrame = {
    //Deduping work depending on the consecutive option
    val dfDedup = keywordsMap.get("consecutive") match {
      //When consecutive, then only neighboring repeating values delete
      case Some(Keyword("consecutive", "true")) =>
        val expr = returns.flatFields.foldLeft(List[Column]()) { (accum: List[Column], item: String) => col(item) :: (lit("#") :: accum) }
        val indexedDf = _df.withColumn("idx", monotonically_increasing_id())
        val dfWithDedupMarkingCol = indexedDf.withColumn("dedupcol", concat(expr: _*))
        val ws = Window.orderBy("idx")
        val dfWithPrevDedupCol = dfWithDedupMarkingCol.withColumn("prevdedup", lag("dedupcol", 1).over(ws))
        dfWithPrevDedupCol.filter(when(col("dedupcol") === col("prevdedup"), false).otherwise(true))
          .drop("dedupcol", "prevdedup", "idx")
      case _ => _df.dropDuplicates(returns.flatFields.map(_.stripBackticks()))
    }
    //Sorting if sortby param exists
    positionalsMap.get("sortby") match {
      case Some(Positional("sortby", sf)) => {
        val sq = SimpleQuery(sf.map(_.stripBackticks()).mkString(" "))
        new OTLSort(sq).transform(dfDedup)
      }
      case _ => dfDedup
    }
  }
}
