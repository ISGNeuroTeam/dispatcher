package ot.scalaotl
package commands

import ot.scalaotl.extensions.DataFrameExt._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ col, lit, explode, array, udf, first }

class OTLTranspose(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("column_name", "header_field", "include_empty")
  override def transform(_df: DataFrame): DataFrame = {

    val keys = returns.flatFields
    val nrows = keys.headOption.getOrElse("5").toInt
    val columnName = getKeyword("column_name").getOrElse("column")
    val headerField = getKeyword("header_field").getOrElse("id")
    val includeEmpty = getKeyword("include_empty").getOrElse("true")

    var limitedDf = _df.limit(nrows)
    if(headerField.equals("id")) limitedDf = limitedDf.withIndex("id")


    val columns = _df.columns

    def zipUdf = udf((cols: Seq[String], vals: Seq[String]) => cols.zip(vals))

    var result = limitedDf.select(col(headerField), zipUdf(lit(columns), array(columns.map(col): _*)).as("temp"))
      .withColumn("rows", explode(col("temp")))
      .select(col(headerField), col("rows._1").as(columnName), col("rows._2").as("value"))
      .groupBy(columnName)
      .pivot(headerField)
      .agg(first("value"))

    if(headerField.equals("id")) {
      val headers = (Array(columnName) ++ (1 to nrows).map("row " + _.toString)).toSeq
      result = result.toDF(headers: _*)
    }
    result
  }
}
