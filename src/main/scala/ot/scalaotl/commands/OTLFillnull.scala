package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.functions.expr

import scala.util.{Failure, Success, Try}
import ot.scalaotl.extensions.StringExt._

import scala.Option

class OTLFillnull(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("value")
  val searchId: Int = sq.searchId

  override def transform(_df: DataFrame): DataFrame = {
    val fillValue = getKeyword("value").getOrElse("0")
    val schema = _df.schema
    val fields = if (returns.flatFields.isEmpty) _df.columns.toList else returns.flatFields
    //val fieldsNoBcktck = fields.map(_.stripBackticks)
    fields.map(_.stripBackticks()).intersect(_df.columns)
        .foldLeft(_df) { (acc, name) =>
          val backtickedName =name.addSurroundedBackticks
          if (schema(name).dataType == StringType)
            acc.withColumn(name, expr(s"ifnull($backtickedName, $fillValue)"))
          else if (schema(name).dataType.isInstanceOf[NumericType])
          {
            Try(fillValue.toDouble)  match {
              case Success (x) => acc.withColumn(name, acc(backtickedName).cast(DoubleType)).withColumn(name, expr(s"ifnull($backtickedName, $fillValue)"))
              case _ => acc
            }
          }
          else acc.withColumn(name, acc(backtickedName).cast(StringType)).withColumn(name, expr(s"ifnull($backtickedName, $fillValue)"))
        }
    }
}
