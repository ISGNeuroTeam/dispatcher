package ot.scalaotl
package commands

import scala.util.Try
import org.apache.spark.sql.{Column, DataFrame}
import ot.scalaotl.extensions.StringExt._
import org.apache.spark.sql.functions._

class OTLReturn(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set.empty[String]

  override def fieldsUsed: List[String] = getFieldsUsed(returns).map(_.stripBackticks.stripPrefix("$").addSurroundedBackticks)
  def isInteger(s: String): Boolean = Try(s.toInt).isSuccess
  val count: Int = args.split(" ").headOption.filter(isInteger) match {
    case Some(str) => str.toInt
    case _         => 1
  }

  val fields = returns.flatFields.filter(_ != count.toString.addSurroundedBackticks)

  override def transform(_df: DataFrame): DataFrame = {
    val nFields = fields.map(_.stripBackticks).filter(_.startsWith("$"))
    val ndf =  nFields.foldLeft(_df)((acc,f) => acc.withColumn(f,  col(f.stripPrefix("$"))))
    val dfLimit = ndf.limit(count).select(fields.head, fields.tail:_*)
    dfLimit
  }
}
