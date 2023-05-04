package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ot.scalaotl.extensions.StringExt._

class OTLJoin(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("subsearch", "max", "type")
  val cachedDFs: Map[String, DataFrame] = sq.cache

  override def transform(_df: DataFrame): DataFrame = {
    val subsearch: String = getKeyword("subsearch").getOrElse("__nosubsearch__")
    if (cachedDFs.contains(subsearch) && returns.flatFields.nonEmpty) {
      val jdf: DataFrame = cachedDFs(subsearch)
      val joinOn = returns.flatFields.map(_.stripBackticks())
      val bothCols = _df.columns.intersect(jdf.columns)
      val notJoinBothCols = bothCols.diff(joinOn)
      val workDf = notJoinBothCols.foldLeft(_df){ case (accum, item) =>
        accum.withColumn(item + "__left__", col(item)).drop(item)
      }
      val workJdf = notJoinBothCols.foldLeft(jdf) { case (accum, item) =>
        accum.withColumn(item + "__right__", col(item)).drop(item)
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
        val intdf = workDf.join(workJdf, joinOn, getKeyword("type").getOrElse("left")).withColumn("__fake__", lit(null))
        val intDfView = intdf.collect()
        var resultDf = notJoinBothCols.foldLeft(intdf) { case (accum, item) =>
          accum.withColumn(item, when(accum(item + "__right__").isNull, col(item + "__left__")).otherwise(col(item + "__right__")))
            .drop(item + "__left__").drop(item + "__right__")
        }
        resultDf = resultDf.drop("__fake__")
        if (getKeyword("max").getOrElse("0") == "1") {
          resultDf.dropDuplicates(joinOn)
        } else {
          val dfView = resultDf.collect
          resultDf
        }
      } else _df
    } else _df
  }
}