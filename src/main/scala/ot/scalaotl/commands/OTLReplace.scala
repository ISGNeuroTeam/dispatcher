package ot.scalaotl
package commands

import com.isgneuro.otl.processors.Replace
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.CustomException.{E00012, E00013}
import ot.scalaotl.parsers.ReplaceParser

class OTLReplace(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("in")) with ReplaceParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override def fieldsUsed: List[String] = getPositional("in").getOrElse(List[String]())

  override def transform(_df: DataFrame): DataFrame = {
    val colToReplace = fieldsUsed.headOption.getOrElse("")
    if (returns.fields.length != 1)
      throw E00012(sq.searchId, "replace", "wc-field")
    val replsTuple = new Tuple2[String, String](returns.fields.head.field, returns.fields.head.newfield)
    if (replsTuple._1 == replsTuple._2) {
      throw E00013(sq.searchId, "replace", replsTuple._1 + ", " + replsTuple._2)
    }
    val worker = new Replace(spark, colToReplace, replsTuple)
    worker.transform(_df)
  }
}