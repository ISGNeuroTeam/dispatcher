package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, expr}
import ot.scalaotl.extensions.StringExt._

/** =Abstract=
 * This class provides support of __'''untable'''__ otl command.
 *
 * __'''untable'''__ used to convert wide table data to a long table.
 * The command allows, having fixed the value of one of the columns (for example, the _time field),
 * to split each of the lines of the source table into several lines.
 *
 * Command syntax untable <fixed_column_name>, <field_column_name>, <value_column_name>
 * First column name is fixed column, second is name of column with metrics names and third is column with metrics values.
 * The first column must be present in the dataset.
 *
 * =Usage example=
 * OTL: generate long table with columns _time, metric_name and value
 * {{{  other otl-commands ... | untable _time, metric_name, value }}}
 *
 * @constructor creates new instance of [[ OTLUntable ]]
 * @param sq [[ SimpleQuery ]] - contains args, cache, subsearches, search time interval, stfe and preview flags
 */
class OTLUntable(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def transform(_df: DataFrame): DataFrame = {
    if (_df.isEmpty)
      return _df
    // get fixed, field and value column names
    val (fixed, field, value) = returns.flatFields match {
      case x :: y :: z :: tail => (
        x.stripBackticks().strip("\""),
        y.stripBackticks().strip("\""),
        z.stripBackticks().strip("\"")
      )
      case _ => return spark.emptyDataFrame
    }
    if (_df.columns.contains(fixed)) {
      // select columns that not in args
      val cols = _df.columns.filter(x => x != fixed && x != field && x != value).map(_.addSurroundedBackticks)
      if (cols.isEmpty) {
        return _df.drop(field, value)
      }
      // for all columns that not in args convert to arrays [column name, value]
      cols.foldLeft(_df) {
        (accum, colname) => {
          accum.withColumn(colname.stripBackticks(), expr(s"""array("${colname.stripBackticks()}", $colname)"""))
        }
      }
        // adding column arr as an array containing all other columns
        .withColumn("arr", expr(s"""array(${cols.mkString(", ")})"""))
        .select(fixed.strip("\""), "arr")
        // explode arr column to separate arrays [column name, value]
        .withColumn("arr", explode(col("arr")))
        // create field and value columns from created arrays
        .withColumn(field.strip("\""), col("arr").getItem(0))
        .withColumn(value.strip("\""), col("arr").getItem(1))
        .drop("arr")
    }
    else
      spark.emptyDataFrame
  }

}
