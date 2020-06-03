package ot.scalaotl.commands.service

import org.apache.spark.sql.DataFrame
import ot.scalaotl.SimpleQuery
import ot.scalaotl.commands.OTLBaseCommand
import ot.scalaotl.extensions.StringExt._

import scala.sys.process._

class ReloadCommand(sq: SimpleQuery) extends OTLBaseCommand(sq){
  override val requiredKeywords: Set[String] = Set()
  override val optionalKeywords: Set[String] = Set()

  override def transform(_df: DataFrame): DataFrame = {
    spark.createDataFrame(Seq(Tuple1("OK"))).withColumnRenamed("_1","reload_status")
  }
}

