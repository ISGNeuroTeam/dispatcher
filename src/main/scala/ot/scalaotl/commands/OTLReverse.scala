package ot.scalaotl
package commands

import ot.scalaotl.extensions.DataFrameExt._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

class OTLReverse(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set.empty[String]

  override def transform(_df: DataFrame): DataFrame = {
    val idxCol = "idx"
    _df.withIndex(idxCol)
      .orderBy(desc(idxCol))
      .dropIndex(idxCol)
  }
}
