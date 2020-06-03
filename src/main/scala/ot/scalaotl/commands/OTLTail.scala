package ot.scalaotl
package commands

import ot.dispatcher.OTLQuery
import org.apache.spark.sql.DataFrame

class OTLTail(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set.empty[String]
  override def fieldsUsed: List[String] = List()
  override def transform(_df: DataFrame): DataFrame = {
    new Converter(OTLQuery(s"| reverse | head ${args}"), cache = Map()).setDF(_df).run
  }
}
