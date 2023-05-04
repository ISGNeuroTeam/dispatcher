package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.StringExt._

class OTLJoin(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("subsearch", "max", "type")
  val cachedDFs: Map[String, DataFrame] = sq.cache

  override def transform(_df: DataFrame): DataFrame = {
    val dfView = _df.collect()
    val subsearch: String = getKeyword("subsearch").getOrElse("__nosubsearch__")
    if (cachedDFs.contains(subsearch) && returns.flatFields.nonEmpty) {
      val jdf: DataFrame = cachedDFs(subsearch)
      val joinOn = returns.flatFields.map(_.stripBackticks())
      val bothCols = _df.columns.intersect(jdf.columns)
      bothCols.foldLeft(_df){ case (accum, item) =>
        val workItem = item.stripBackticks
        accum.withSafeColumnRenamed(workItem, workItem + "__left__")
      }
      bothCols.foldLeft(jdf) { case (accum, item) =>
        val workItem = item.stripBackticks
        accum.withSafeColumnRenamed(workItem, workItem + "__right__")
      }
      log.debug(f"[SearchId:${sq.searchId}] Columns in right df: ${jdf.columns.mkString(", ")}")
      log.debug(f"[SearchId:${sq.searchId}] Columns join on: ${joinOn.mkString(", ")}")
      log.debug(f"[SearchId:${sq.searchId}] Columns in both dashboards: ${bothCols.mkString(", ")}")
      if (joinOn.forall(bothCols.contains)) {
        val dfSuffix = _df
        /*if (jdf.isEmpty) _df //TODO find more convenient vay to check if df is empty
        else bothCols.diff(joinOn).foldLeft(_df) { case (accum, item) =>
          accum.drop(item)
        }*/
        val suffView = dfSuffix.collect()
        val jdfView = jdf.collect()
        val intdf = dfSuffix.join(jdf, joinOn, getKeyword("type").getOrElse("left")).withColumn("__fake__", lit(null))
        val intDfView = intdf.collect()
        bothCols.foldLeft(intdf) { case (accum, item) =>
          accum.withColumn(item, when(accum(item + "__right__") === accum("__fake__"), col(item + "__left__")).otherwise(col(item + "__right__")))
          accum.drop(item + "__left").drop(item + "__right__")
        }
        intdf.drop("__fake__")
        if (getKeyword("max").getOrElse("0") == "1") {
          intdf.dropDuplicates(joinOn)
        } else {
          val dfView = intdf.collect
          intdf
        }
      } else _df
    } else _df
  }
}