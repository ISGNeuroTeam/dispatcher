package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, last, lit, monotonically_increasing_id}
import ot.scalaotl.extensions.StringExt._

/** =Abstract=
 * This class provides support of __'''filldown'''__ otl command.
 *
 * __'''filldown'''__ changes __NULL__ values with the last non-null value for a field
 *
 * __'''filldown'''__ takes two optional argument:
 *
 *    1.  _'''wc-field-list'''_ - comma-separated field names to which the command will apply. If this param
 *    is empty, than command will apply to all NULL-value-containing fields
 *    2. __'''by'''__ - defines a field, according to the value of which the table is split into parts,
 *    within which the NULL values of the fields defined in parameter 1 are filled with the last non-NULL value
 *    of the same fields - without using the values of the fields defined in other parts.
 *
 * Note: if _'''by'''_ is used, the parameter _'''wc-field-list'''_ must be specified
 *
 * =Usage examples=
 * OTL 1:
 * {{{| makeresults | eval a = mvappend(1,NULL,2,NULL,NULL) | eval b = mvappend(3,NULL,NULL,4,NULL) | mvexpand a
 *| filldown}}}
 * Result:
 * {{{+----------+---+---+
|     _time|  b|  a|
+----------+---+---+
|1646048207|  3|1.0|
|1646048207|  3|1.0|
|1646048207|  3|2.0|
|1646048207|  4|2.0|
|1646048207|  4|2.0|
+----------+---+---+}}}
 * OTL 2:
 * {{{| makeresults | eval a = mvappend(1,NULL,2,NULL,NULL) | eval b = mvappend(3,NULL,NULL,4,NULL) | mvexpand a
 *| filldown b}}}
 * Result:
 * {{{+----------+---+---+
|     _time|  b|  a|
+----------+---+---+
|1646048207|  3|1.0|
|1646048207|  3|NULL|
|1646048207|  3|2.0|
|1646048207|  4|NULL|
|1646048207|  4|NULL|
+----------+---+---+}}}
 * OTL 3:
 * {{{| makeresults | eval a = mvappend(1,NULL,2,NULL,NULL) | eval b = mvappend(3,NULL,NULL,4,NULL) | eval id = mvappend(10, 10, 20, 20, 20) | mvexpand a
 *| filldown b by id}}}
 * Result:
 * {{{+----------+---+---+
|     _time|  b|  a| id|
+----------+---+---+---+
|1646048207|  3|1.0|10|
|1646048207|  3|NULL|10|
|1646048207|  NULL|2.0|20|
|1646048207|  4|NULL|20|
|1646048207|  4|NULL|20|
+----------+---+---+}}}
 * OTL 4:
 * {{{| makeresults | eval a = mvappend(1,NULL,2,NULL,NULL) | eval b = mvappend(3,NULL,NULL,4,NULL) | eval id = mvappend(10, 10, 20, 20, 20) | mvexpand a
 *| filldown b, a by id}}}
 * Result:
 * {{{+----------+---+---+
|     _time|  b|  a| id|
+----------+---+---+---+
|1646048207|  3|1.0|10|
|1646048207|  3|1.0|10|
|1646048207|  NULL|2.0|20|
|1646048207|  4|2.0|20|
|1646048207|  4|2.0|20|
+----------+---+---+}}}
 * @constructor creates new instance of [[OTLFilldown]]
 * @param sq [[SimpleQuery]]
 */
class OTLFilldown(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  /**
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return _df with events combined by specified field
   */
  //All comment in this method in feature/filldown-command-normalization are technical and should be delete at merging
  override def transform(_df: DataFrame): DataFrame = {
    //define values of by parameter: if by exists in command, then it's values; else - fictive parameter __internal__
    val groups = positionalsMap.get("by") match {
      case Some(Positional("by", groups)) => groups
      case _ => List("__internal__")
    }
    //define one by-marked field for further actions: if earlier defined values are empty, then fictive, else - first value
    val by = if (groups.isEmpty) {
      "__internal__"
    } else {
      groups.head.stripBackticks()
    }
    //Spark-window for partitioning action defining:
    // Windows will be part by 'by-field'
    val ws1 = Window.partitionBy(by)
    //This will ordering by increasing with step 1 int field __idx__ in every part
    val ws2 = ws1.orderBy("__idx__")
    //Specifying boundaries: from first row in the parttion to current row
    val ws = ws2.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val dfColumns = _df.columns
    //Defining fields for filldown operation: if  _'''wc-field-list'''_ non empty, then these fields; else - define fields with null values in incoming dataframe
    val fields = if (returns.flatFields.isEmpty) {
      dfColumns.filter(c => !(_df.select(col(c)).filter(row => row.isNullAt(0)).isEmpty)).toList
    } else {
      returns.flatFields
    }
    //filldown columns, gived by user, should be in space of dataframe columns => do intersect
    val filldownColumns = fields.map(_.stripBackticks()).intersect(dfColumns)
    log.debug(s"filldownColumns $filldownColumns")
    //adding fictive __internal__ column with literal '0' value in all rows. It's do for Window.partitionBy operation (r. 100) in case of by-param missing -
    // then count of parts: 1 (full dataframe as part)
    val df_grouped = _df.withColumn("__internal__", lit(0))
    //Reducing operation. Receive dataframe without filldown applying, function for dataframe updating by n (count of filldownColumns) steps and return dataframe with filldown applying
    //In start - adding of __idx column to dataframe (see r. 102)
    filldownColumns.foldLeft(df_grouped.withColumn("__idx__", monotonically_increasing_id)) {
            //foldLeft inside work as cycle:
          // accum - start dataframe from parameter 1 in foldLeft function or transformed after operation applying dataframe
          // item - next value in filldownColumns in order from head to tail
      case (accum, item) => {
        //preColumn and column are not really created after initializing and before using in .withColumn method columns - it's containers of expressions
        //from [last] doc:
        //Aggregate function: returns the last value in a group.
        //The function by default returns the last values it sees. It will return the last non-null value it sees when ignoreNulls is set to true.
        // If all values are null, then null is returned.
        //last will work in all filldown columns and fill not-null values as-is, null values - by last not-null
        //Ex. of preColumn inner expression: last('field, true)
        val preColumn = last(col(item), ignoreNulls = true)
        //It means that operation of filling by last, defining in preColumn, should processing in boundaries of earlier defined Window ws with partitions and
        //ordering inside parts, where partition defined by 'by-param' value
        //Ex. of column inner expression: last('field, true) windowspecdefinition('ID, '__idx__ ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$()))
        val column = preColumn.over(ws)
        //If column with name as in oaram. 1 is exists, then this column replacing by column, defined in param. 2 and applying of all operations, defined in column expression for values
        val result = accum.withColumn(item, column)
        //If all filldownColumns was processed, then return final result, else -> to next step
        result
      }
    }
      //dropping fictive columns
      .drop("__idx__", "__internal__")
  }
}
