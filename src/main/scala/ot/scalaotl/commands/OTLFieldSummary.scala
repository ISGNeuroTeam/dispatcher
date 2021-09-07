package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

class OTLFieldSummary(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  override val fieldsGenerated = List("summary")
  def zipUdf: UserDefinedFunction = udf((cols: Seq[String], vals: Seq[String]) => cols.zip(vals))

  override def transform(_df: DataFrame): DataFrame = {

    val columnName = getKeyword("column_name").getOrElse("column")

    val columns = _df.columns

    _df.describe()
      .select(col("summary"), zipUdf(lit(columns), array(columns.map(col): _*)).as("temp"))
      .withColumn("rows", explode(col("temp")))
      .select(col("summary"), col("rows._1").as(columnName), col("rows._2").as("value"))
      .groupBy(columnName)
      .pivot("summary")
      .agg(first("value"))
      //.toDF(headers: _*)
  }
}