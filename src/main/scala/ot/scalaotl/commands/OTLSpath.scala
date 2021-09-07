package ot.scalaotl
package commands

import ot.scalaotl.static.OtJsonParser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ col, lit }
import org.apache.spark.sql.functions.json_tuple

import scala.annotation.tailrec

class OTLSpath(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("input", "path", "output")

  override def transform(_df: DataFrame): DataFrame = {
    val inputField = getKeyword("input").getOrElse("_raw")
    if (_df.columns.contains(inputField)) {
      val path = getKeyword("path").getOrElse(returns.flatFields.headOption.getOrElse(return _df))
      val outputField = getKeyword("output").getOrElse(path)
      _df.withColumn("_ot_path", lit(path))
        .withColumn(outputField, OtJsonParser.spathUDF(col(inputField), col("_ot_path")))
        .drop("_ot_path")
    } else _df
  }
}

class OTLSpathNoUDF(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("input", "path", "output")

  def getJsonField(df: DataFrame, input: String, output: String, path: List[String]): DataFrame = {
    @tailrec
    def getJsonInner(dfInner: DataFrame, colname: String, path: List[String]): DataFrame = path match {
      case head :: tail => getJsonInner(dfInner.withColumn(colname, json_tuple(col(colname), head)), colname, tail)
      case _            => dfInner
    }
    getJsonInner(df.withColumn(output, col(input)), output, path)
  }

  override def transform(_df: DataFrame): DataFrame = {
    val inputField = getKeyword("input").getOrElse("_raw")
    if (_df.columns.contains(inputField)) {
      val path = getKeyword("path").getOrElse(returns.flatFields.headOption.getOrElse(return _df))
      val outputField = getKeyword("output").getOrElse(path)
      getJsonField(_df, inputField, outputField, path.split("\\.").toList)
    } else _df
  }
}
