package ot.scalaotl
package commands

import ot.scalaotl.parsers.ReplaceParser
import ot.scalaotl.static.OtDatetime

import org.apache.spark.sql.functions.{ lit, expr, min, max }
import org.apache.spark.sql.DataFrame

class OTLBin(sq: SimpleQuery) extends OTLBaseCommand(sq) with ReplaceParser {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("bins","span")
  override def transform(_df: DataFrame): DataFrame = {
    val ReturnField(newfield, field) = returns.fields.headOption.getOrElse(return _df)
    val dfWithMinMax = _df.crossJoin(_df.agg(min(field).alias("__min__"), max(field).alias("__max__")))

    val binSpanFunc: (DataFrame => DataFrame) = if (keywordsMap.contains("bins")) {
      val bins = getKeyword("bins").getOrElse("1").toInt
      (df: DataFrame) => df.withColumn("__bins__", lit(bins)).withColumn("__span__", expr("""(__max__ - __min__) / __bins__"""))
    } else if (keywordsMap.contains("span")) {
      val span = OtDatetime.getSpanInSeconds(getKeyword("span").getOrElse("10000d"))
      (df: DataFrame) => df.withColumn("__span__", lit(span))
    } else {
      throw CustomException(6, sq.searchId, s"Command ${commandname} requires 'bins' or 'span' argument", List(commandname,"'bins','span'" ))
    }

    dfWithMinMax
      .transform(binSpanFunc)
      .withColumn(newfield, expr(s"""$field - (($field - __min__) % __span__)"""))
      .drop("__min__", "__max__", "__span__", "__bins__")
  }
}
