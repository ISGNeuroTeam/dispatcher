package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import ot.scalaotl.commands.commonconstructions.StatsTransformer

/** =Abstract=
 * This class provides support of __'''transaction'''__ otl command.
 *
 * __'''transaction'''__ used to find first record with unique set of specified fields.
 * The command allows to find among the many records grouped by the specified set of fields, find the first record by time.
 * The specified set of fields is called a transaction
 * For correct operation, the presence of the _ field in data is required
 *
 * Command syntax transaction [field-list]
 * The use of this command with a data without the _time field, as well as the inclusion of this field in the list
 * of command parameters, is wrong
 *
 * =Usage example=
 * OTL: return first transaction (by time) that includes fields object, metric_name, value
 * {{{  other otl-commands ... | transaction object, metric_name, value }}}
 *
 * @constructor creates new instance of [[ OTLTransaction ]]
 * @param sq [[ SimpleQuery ]] - contains args, cache, subsearches, search time interval, stfe and preview flags
 */
class OTLTransaction(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  /**
   * Transformer with logic of stats command
   */
  val statsTransformer = new StatsTransformer(Right(SimpleQuery(s"values(*) as * by $args").args), spark)

  def mapUdf: UserDefinedFunction = udf { (col: Seq[Seq[String]]) => col.map(x => (x.head, x(1))).toMap }

  def createMap(df: DataFrame, columns: List[String] = List()): DataFrame = {
    val cols = if (columns.isEmpty) df.columns.toList else columns
    val dfArr = cols.foldLeft(df) {
      case (accum, colname) => accum.withColumn(colname, array(lit(colname), col(colname)))
    }
    val colStr = cols.mkString(", ")
    val dfTotal = dfArr.withColumn("total", expr(s"""array($colStr)"""))
    dfTotal.withColumn("total", mapUdf(col("total"))).select("total")
  }

  def convertArraysToSingle(df: DataFrame): DataFrame = {
    val sch = df.schema
    val colsInit = df.columns.filter(sch(_).dataType.typeName == "array")
    // if df not contains array type columns then return df
    if (colsInit.nonEmpty) {
      val funcs = colsInit.map(x => collect_set(size(col(x))).alias(x))
      val dfLen = colsInit.foldLeft(df.agg(funcs.head, funcs.tail: _*)) {
        case (accum, colname) => accum.withColumn(colname, size(col(colname)))
      }
      val mapCols = createMap(dfLen).select("total").collect.head.get(0).asInstanceOf[Map[String, Int]]
      colsInit.foldLeft(df) {
        case (accum, item) => if (mapCols(item).toString == "1") accum.withColumn(item, col(item)(0)) else accum
      }
    }
    else
      df
  }

  override def transform(_df: DataFrame): DataFrame = {
    if (_df.isEmpty)
      return _df
    val dfTransaction = convertArraysToSingle(statsTransformer.transform(_df))
    // if _time not exists in original df then return empty dataframe
    if (dfTransaction.columns.contains("_time")){
      if (dfTransaction.schema("_time").dataType.typeName == "array")
        dfTransaction.withColumn("_time", array_min(col("_time")))
      else
        dfTransaction
    }
    else
      spark.emptyDataFrame
  }
}
