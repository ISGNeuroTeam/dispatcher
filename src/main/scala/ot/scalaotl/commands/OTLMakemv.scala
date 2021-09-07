package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ col, split }
import ot.scalaotl.extensions.StringExt._

class OTLMakemv(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("delim")
  override def transform(_df: DataFrame): DataFrame = {
    val delim = getKeyword("delim").getOrElse("\\s").strip("\"")
    returns.flatFields match {
      case head :: tail => return _df.withColumn(head.strip("`"), split(col(head), delim))
      case _            => return _df
    }
  }
}
