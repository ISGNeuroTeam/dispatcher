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
  }
}
