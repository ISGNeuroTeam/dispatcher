package ot.scalaotl
package commands

import com.isgneuro.otl.processors.Makeresults
import org.apache.spark.sql.DataFrame

class OTLMakeresults(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLSparkSession {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("annotate", "count")

  val annotate = if (getKeyword("annotate").getOrElse("false") == "true") true else false

  override val fieldsGenerated: List[String] = if (annotate) {
    List("_raw", "host", "source", "sourcetype")
  } else List("_time")

  override def transform(_df: DataFrame): DataFrame = {
    val makeresulter = new Makeresults(spark, getKeyword("count").getOrElse("1").toInt, annotate)
    makeresulter.transform(_df)

    /*val sch = StructType(
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
    } else df*/
  }
}
