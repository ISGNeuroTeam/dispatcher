package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, lit, sum}
import ot.scalaotl.extensions.StringExt._

class OTLAddtotals(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("row", "col", "fieldname", "labelfield", "label")

  override def transform(_df: DataFrame): DataFrame = {

    val isRow = getKeyword("row").getOrElse("true").toBoolean
    val isCol = getKeyword("col").getOrElse("false").toBoolean
    val fieldname = getKeyword("fieldname").getOrElse("Total")

    val labelfield = getKeyword("labelfield").getOrElse("None") //if col=true
    val label = getKeyword("label").getOrElse("Total") //if col=true & labelfield is set
    val fieldlist = if (returns.flatFields.isEmpty) _df.columns.toList else returns.flatFields
    val numericCols = fieldlist.map(x => ("__casted_" + x.stripBackticks).addSurroundedBackticks).toArray

    val cdf = fieldlist.foldLeft(_df) { case (accum, item) => accum.withColumn("__casted_" + item.stripBackticks, _df(item).cast(DoubleType)) }

    def getColStats(df: DataFrame, numeric: Array[String], nonNumeric: Array[String]): DataFrame = {
      val aggs = numeric.map(name => sum(col(name)).as(name.stripBackticks()))
      nonNumeric.foldLeft(df.agg(aggs.head, aggs.tail: _*))((data, colName) => {
        data.withColumn(colName, lit(null))
      })
    }

    {
      if (isRow) {
        if (isCol) {
          val rowStats = cdf.withColumn(fieldname, numericCols.map(col).reduce(_ + _))
          val numeric = numericCols ++ Array(fieldname)
          val other = cdf.columns.diff(numeric.map(_.stripBackticks()))

          if (labelfield == "None") { //TODO
            val colStats = getColStats(rowStats, numeric, other)
            val origColStats = numericCols.foldLeft(colStats) { case (accum, item) => accum.withColumn(item.stripBackticks().stripPrefix("__casted_"), accum(item)) }
            rowStats.unionByName(origColStats)
          } else {
            val labeledDf = rowStats.withColumn(labelfield, lit(null))
            val lcolStats = getColStats(labeledDf, numeric, other)
            val origColStats = numericCols.foldLeft(lcolStats) { case (accum, item) => accum.withColumn(item.stripBackticks().stripPrefix("__casted_"), accum(item)) }
            labeledDf.unionByName(origColStats.withColumn(labelfield, lit(label)))
          }
        } else {
          cdf.withColumn(fieldname, numericCols.map(col).reduce(_ + _))
        }
      } else {
        if (isCol) {
          val other = cdf.columns.diff(numericCols.map(_.stripBackticks()))

          if (labelfield == "None") {
            val colStats = getColStats(cdf, numericCols, other)
            val origColStats = numericCols.foldLeft(colStats) { case (accum, item) => accum.withColumn(item.stripBackticks().stripPrefix("__casted_"), accum(item)) }
            cdf.unionByName(origColStats)
          } else {
            val labeledDf = cdf.withColumn(labelfield, lit(null))
            val lcolStats = getColStats(labeledDf, numericCols, other)
            val origColStats = numericCols.foldLeft(lcolStats) { case (accum, item) => accum.withColumn(item.stripBackticks().stripPrefix("__casted_"), accum(item)) }
            labeledDf.unionByName(origColStats).withColumn(labelfield, lit(label))
          }
        } else {
          _df
        }
      }
    }.drop(numericCols.map(_.stripBackticks()): _*)
  }
}
