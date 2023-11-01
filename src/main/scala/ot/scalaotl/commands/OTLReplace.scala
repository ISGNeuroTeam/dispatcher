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
    val replsMap = {
      for {e <- returns.fields}
        yield e.field -> e.newfield
    }.toMap
    if (replsMap.isEmpty) {
      throw E00012(sq.searchId, "replace", "wc-field")
    }
    if (replsMap.forall(f => f._1 == f._2)) {
      throw E00013(sq.searchId, "replace", replsMap.keys.mkString(", "))
    }
    val worker = new Replace(colToReplace, replsMap)
    worker.transform(_df)
  }
}