package ot.scalaotl
package commands
package local

import scala.util.matching.Regex
import org.apache.spark.sql.functions.{ col, udf }
import org.apache.spark.sql.DataFrame

class Long2IP(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set.empty[String]

  val fields: List[String] = {
    val rexTextBetweenQuotes: Regex = """(?:".*?"|[^,\s])+""".r
    rexTextBetweenQuotes.findAllIn(args).toList.map(x => x.trim.stripPrefix("\"").stripSuffix("\""))
  }

  val longToIP = (long: Long) => {
    (0 until 4).map(a => long / math.pow(256, a).floor.toInt % 256).reverse.mkString(".")
  }

  val convertIP = udf(longToIP)

  override def transform(_df: DataFrame): DataFrame = {
    fields.foldLeft(_df) { case (a, b) => a.withColumn(b, convertIP(col(b))) }
  }
}
