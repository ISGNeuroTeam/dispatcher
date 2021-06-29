
package ot.scalaotl
package commands

import org.apache.spark.sql.functions.{col, explode, slice, lit}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.ArrayType
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.extensions.DataFrameExt._

class OTLMvexpand(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("limit")
  override def transform(_df: DataFrame): DataFrame = {
    val limit = getKeyword("limit").getOrElse("0").toInt
    returns.flatFields match {
      case field :: tail =>
        val colToExplode = if (limit > 0) slice(col(field), 1, limit) else col(field)
        val fieldNoBcktck = field.stripBackticks()
        val fieldType = _df.schema.toList.collectFirst{case x if x.name==fieldNoBcktck => x.dataType.typeName}.getOrElse("")
        val f = fieldType match {
          case "array" => explode(colToExplode)
          case _ => colToExplode
        }
        _df.withColumn("exploded_" + fieldNoBcktck, f)
          .drop(fieldNoBcktck)
          .withSafeColumnRenamed("exploded_" + fieldNoBcktck, fieldNoBcktck)
      case _ => _df
    }
  }
}
