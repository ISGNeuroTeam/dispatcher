package ot.scalaotl
package commands

import ot.scalaotl.extensions.StringExt._

import org.apache.spark.sql.functions.{col, lit, count, row_number, desc}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

class OTLTop(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  override val fieldsGenerated = List("percent")

  override def transform(_df: DataFrame): DataFrame = {

    val limit = args.split(" ").headOption match {
      case Some(lim) => lim.toIntSafe match {
        case Some(v) => v
        case _ => 10
      }
      case _ => return _df
    }

    val fields = returns.flatFields.filter(_.stripBackticks() != limit.toString)

    val groups = getPositional("by") match {
      case None | Some(List()) => List()
      case Some(l) => l.map(s => s.stripBackticks())
    }

    val dfCount = groups ++ fields match {
      case head :: tail =>
        _df.groupBy(head, tail: _*).agg(count("*").alias("count"))
      case _ => return _df
    }

    val w = groups match {
      case h :: t => Window.partitionBy(h, t: _*).orderBy(desc("count"))
      case _ => Window.orderBy(desc("count"))
    }

    val dfLimit = dfCount
      .withColumn("rn", row_number.over(w))
      .filter(col("rn") <= limit)
      .drop("rn")

    val dfJoined = groups match {
      case h :: t =>
        val jdf = _df.groupBy(h, t: _*).agg(count("*").alias("total"))
        dfLimit.join(jdf, groups)
      case _ =>
        val jdf = _df.agg(count("*").alias("total"))
        dfLimit.crossJoin(jdf)
    }

    dfJoined
      .withColumn("percent", lit(100) * col("count") / col("total"))
      .drop("total")
  }
}
