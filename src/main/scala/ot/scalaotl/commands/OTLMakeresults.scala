package ot.scalaotl
package commands

import ot.scalaotl.static.OtDatetime

import org.apache.spark.sql.functions.{explode, col, lit}
import org.apache.spark.sql.types.{StructType, StructField, ArrayType, LongType}
import org.apache.spark.sql.{Row, DataFrame}

/** =Abstract=
 * This class provides support of __'''makeresults'''__ otl command.
 *
 * __'''makeresults'''__ generates the given number of search strings.
 *
 * __'''makeresults'''__ takes two optional arguments:
 *
 *  1. __count__ - number of lines to generate,
 *  if __count__ is not specified then __count__ = 1
 *
 *  1. __annotate__ - specifies whether to add columns `_raw`, `host`, `source`, `sourcetype`.
 *  Can be set in two ways:
 *  {{{annotate=t //additional columns will be added}}}
 *  {{{annotate=f //additional columns will not be added}}}
 *  if __annotate__ is not specified then __annotate__ = f
 *
 *  '''_time''' column stores makeresults run time in seconds
 *
 *  and exists regardless of the parameters.
 *
 *  =Usage example=
 *  ==Example 1==
 *  OTL:
 *  {{{| makeresults count=5 annotate=t }}}
 *  Result:
 *  {{{+----------+----+----+------+----------+
|     _time|_raw|host|source|sourcetype|
+----------+----+----+------+----------+
|1646224585|    |    |      |          |
|1646224585|    |    |      |          |
|1646224585|    |    |      |          |
|1646224585|    |    |      |          |
|1646224585|    |    |      |          |
+----------+----+----+------+----------+}}}
 *
 * ==Example 2==
 * OTL:
 * {{{| makeresults }}}
 * Result:
 * {{{+----------+
|     _time|
+----------+
|1646225136|
+----------+}}}
 */
class OTLMakeresults(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLSparkSession {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("annotate", "count")

  override val fieldsGenerated: List[String] = if (getKeyword("annotate").getOrElse("f") == "t") {
    List("_raw", "host", "source", "sourcetype")
  } else List("_time")

  /**
   * @param _df
   * Because makeresults is a generator command, input _df is ignored.
   */
  override def transform(_df: DataFrame): DataFrame = {
    val sch = StructType(
      List(
        StructField("_time", ArrayType(LongType, containsNull = false), nullable = true)))

    val cnt = getKeyword("count").getOrElse("1").toInt
    val unixTimestamp: Long = OtDatetime.getCurrentTimeInSeconds()
    val tsArr = Array.fill(cnt)(unixTimestamp)
    val rdd = spark.sparkContext.makeRDD(Seq(Row(tsArr)))
    val df = spark.createDataFrame(rdd, sch).withColumn("_time", explode(col("_time")))
    if (getKeyword("annotate").getOrElse("f") == "t") {
      val nullCol = lit("")
      fieldsGenerated.foldLeft(df)((a, b) => a.withColumn(b, nullCol))
    } else df
  }
}
