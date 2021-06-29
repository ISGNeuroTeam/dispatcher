package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ udf, col, lit, collect_set, array, size, expr, array_min }

class OTLTransaction(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  def mapUdf = udf { (col: Seq[Seq[String]]) => col.map(x => (x(0), x(1))).toMap }
  def createMap(df: DataFrame, columns: List[String] = List()): DataFrame = {
    val cols = if (columns.isEmpty) df.columns.toList else columns
    val dfArr = cols.foldLeft(df) {
      case (accum, colname) => accum.withColumn(colname, array(lit(colname), col(colname)))
    }
    val colStr = cols.mkString(", ")
    val dfTotal = dfArr.withColumn("total", expr(s"""array(${colStr})"""))
    dfTotal.withColumn("total", mapUdf(col("total"))).select("total")
  }

  def convertArraysToSingle(df: DataFrame): DataFrame = {
    val sch = df.schema
    val colsInit = df.columns.filter(sch(_).dataType.typeName == "array")
    val funcs = colsInit.map(x => collect_set(size(col(x))).alias(x))
    val dfLen = colsInit.foldLeft(df.agg(funcs.head, funcs.tail: _*)) {
      case (accum, colname) => accum.withColumn(colname, size(col(colname)))
    }
    val mapCols = createMap(dfLen).select("total").collect.head.get(0).asInstanceOf[Map[String, Int]]
    colsInit.foldLeft(df) {
      case (accum, item) => if (mapCols(item).toString == "1") accum.withColumn(item, col(item)(0)) else accum
    }
  }

  override def transform(_df: DataFrame) = {
    val s = SimpleQuery(s"values(*) as * by $args")
    val dfTransaction = convertArraysToSingle(new OTLStats(s).transform(_df))
    if (dfTransaction.columns.contains("_time")) {
      dfTransaction.withColumn("_time", array_min(col("_time")))
    } else dfTransaction
  }
}
