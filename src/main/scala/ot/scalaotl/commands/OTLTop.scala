package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import ot.scalaotl.extensions.StringExt._

/** =Abstract=
 * This class provides support of __'''top'''__ otl command.
 *
 * __'''top'''__ finds the most frequently occurring values in the selected fields of event,
 *                calculates the number and percentage of how often the selected values occur in events.
 *
 * __'''top'''__ takes one required and two optional arguments:
 *
 * Required argument:
 *
 *    1.  _'''field-list'''_ - comma-separated field names to which the command will apply.
 *
 * Optional arguments:
 *
 *    1. __'''number'''__ - the number of top rows to be output in the result. The default is 10.
 *    If value of this argument is 0, than all rows will be output in the result.
 *
 *    2.__'''by'''__ - defines the field by which grouping occurs in the table.
 *
 * Top options
 *
 * 1. __'''countfield'''__ - specifies the name of the field that contains the count of the events of the value. Default value: __'''count'''__.
 *
 * 2. __'''percentfield'''__ - specifies the name of the field that contains the percentage of the events that have the value. Default value: __'''percent'''__.
 *
 * 3. __'''showcount'''__ - boolean value, specify whether to create a field called "count". Default value: __'''true'''__.
 *
 * 4. __'''showperc'''__ - boolean value, specify whether to create a field called "percent". Default value: __'''true'''__.
 *
 * =Usage examples=
 * * OTL 1:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | top a}}}
 * Result:
 * {{{+------+--------+
|    a| count| percent|
+---  +------+--------+
|   10|     3|    37.5|
|   20|     2|      25|
|   30|     2|      25|
|   40|     1|    12.5|
+-----+------+--------+}}}
 * OTL 2:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | top 2 a}}}
 * Result:
 * {{{+------+--------+
|    a| count| percent|
+---  +------+--------+
|   10|     3|    37.5|
|   20|     2|      25|
+-----+------+--------+}}}
 * OTL 3:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | top a b}}}
 * Result:
 * {{{+---+------+--------+
|    a|  b| count| percent|
+---  +---+------+--------+
|   40|400|     1|    12.5|
|   30|200|     1|    12.5|
|   30|400|     1|    12.5|
|   10|300|     1|    12.5|
|   10|200|     1|    12.5|
|   10|500|     1|    12.5|
|   20|300|     1|    12.5|
|   20|700|     1|    12.5|
+-----+---+------+--------+}}}
 * OTL 4:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | top 0 b}}}
 * Result:
 * {{{+------+--------+
|    b| count| percent|
+---  +------+--------+
|  300|     2|      25|
|  400|     2|      25|
|  200|     2|    12.5|
|  500|     1|    12.5|
|  700|     1|    12.5|
+-----+------+--------+}}}
 * OTL 5:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | top 2 countfield=ElemsSize a}}}
 * Result:
 * {{{+------+--------+
|    a| ElemsSize| percent|
+---  +------+--------+
|   10|     3|    37.5|
|   20|     2|      25|
+-----+------+--------+}}}
 * * OTL 6:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | top percentfield=procent a}}}
 * Result:
 * {{{+------+--------+
|    a| count| procent|
+---  +------+--------+
|   10|     3|    37.5|
|   20|     2|      25|
|   30|     2|      25|
|   40|     1|    12.5|
+-----+------+--------+}}}
 * * OTL 7:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | top showcount=false a}}}
 * Result:
 * {{{+--------+
|    a| percent|
+---  +--------+
|   10|    37.5|
|   20|      25|
|   30|      25|
|   40|    12.5|
+-----+--------+}}}
 * OTL 8:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | top 0 showperc=false b}}}
 * Result:
 * {{{+------+
|    b| count|
+---  +------+
|  300|     2|
|  400|     2|
|  200|     2|
|  500|     1|
|  700|     1|
+-----+------+}}}
 * @constructor creates new instance of [[OTLTop]]
 * @param sq [[SimpleQuery]]
 */
class OTLTop(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  override val fieldsGenerated = List(getKeyword("countfield").getOrElse("count"), getKeyword("percentfield").getOrElse("percent"))

  /**
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return _df with count and percentage for most frequently occurring values in the selected fields of event
   */
  override def transform(_df: DataFrame): DataFrame = {
    val countfield = fieldsGenerated(0)
    val percentfield = fieldsGenerated(1)
    //Define limit
    val limit = args.split(" ").headOption match {
      case Some(lim) => lim.toIntSafe match {
        case Some(v) => if (v == 0) _df.count else v
        case _ => 10
      }
      case _ => return _df
    }
    val fields = returns.flatFields.filter(_.stripBackticks() != limit.toString)
    val groups = getPositional("by") match {
      case None | Some(List()) => List()
      case Some(l) => l.map(s => s.stripBackticks())
    }
    //Dataset, grouping by top-applying columns or by 'by'-param column + top-applying columns with adding column of count by each group
    val dfCount = groups ++ fields match {
      case head :: tail =>
        _df.groupBy(head, tail: _*).agg(count("*").alias(countfield))
      case _ => return _df
    }
    //Windowed func spec for cases of 'by'-param existing and not existing
    val w = groups match {
      case h :: t => Window.partitionBy(h, t: _*).orderBy(desc(countfield))
      case _ => Window.orderBy(desc(countfield))
    }
    //Limiting of entries: if 'by'-param exists, limiting in each group
    val dfWindowed = dfCount.withColumn("rn", row_number.over(w))
    val dfLimit = dfWindowed.filter(col("rn") <= limit)
      .drop("rn")
    //Defining of total count of entries in dataset or in each group (if 'by-param exists') and joining limited dataset with totals-containing dataset
    val dfJoined = groups match {
      case h :: t =>
        val jdf = _df.groupBy(h, t: _*).agg(count("*").alias("total"))
        dfLimit.join(jdf, groups)
      case _ =>
        val jdf = _df.agg(count("*").alias("total"))
        dfLimit.crossJoin(jdf)
    }
    //Defining percents
    val dfResult = keywordsMap.get("showperc") match {
      case Some(Keyword("showperc", "false")) => dfJoined.drop("total")
      case Some(Keyword("showperc", "true")) | None =>
        dfJoined.withColumn(percentfield, lit(100) * col(countfield) / col("total"))
          .drop("total")
    }
    keywordsMap.get("showcount") match {
      case Some(Keyword("showcount", "false")) => dfResult.drop(countfield)
      case Some(Keyword("showcount", "true")) | None => dfResult
    }
  }
}