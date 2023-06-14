package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ot.scalaotl.extensions.StringExt._

class OTLJoin(sq: SimpleQuery) extends OTLBaseCommand(sq) {

  val leftSide = "__left"
  val rightSide = "__right__"

  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("subsearch", "max", "type")

  val cachedDFs: Map[String, DataFrame] = sq.cache
  val subsearch: String = getKeyword("subsearch").getOrElse("__nosubsearch__")

  var notJoinBothCols: Array[String] = Array()

  override def transform(_df: DataFrame): DataFrame = {
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    if (cachedDFs.contains(subsearch) && returns.flatFields.nonEmpty) {
      val jdf: DataFrame = cachedDFs(subsearch)
      val joinOn = returns.flatFields.map(_.stripBackticks())
      val bothCols = _df.columns.intersect(jdf.columns)
      log.debug(f"[SearchId:${sq.searchId}] Columns in right df: ${jdf.columns.mkString(", ")}")
      log.debug(f"[SearchId:${sq.searchId}] Columns join on: ${joinOn.mkString(", ")}")
      log.debug(f"[SearchId:${sq.searchId}] Columns in both dataframes: ${bothCols.mkString(", ")}")
      if (joinOn.forall(bothCols.contains)) {
        notJoinBothCols = bothCols.diff(joinOn.union(List("_raw")))
        val preWorkDf = if (!jdf.isEmpty && jdf.columns.contains("_raw")) _df.drop("_raw") else _df
        val workDf = createStructChangedDf(preWorkDf, leftSide)
        val workJdf = createStructChangedDf(jdf, rightSide)
        val joinedDf = workDf.join(workJdf, joinOn, getKeyword("type").getOrElse("left"))
        val resultDf = notJoinBothCols.foldLeft(joinedDf) { case (accum, item) =>
          accum.withColumn(item, when(accum(item + rightSide).isNull, col(item + leftSide)).otherwise(col(item + rightSide)))
            .drop(item + leftSide).drop(item + rightSide)
        }
        if (getKeyword("max").getOrElse("0") == "1") {
          resultDf.dropDuplicates(joinOn)
        } else {
          resultDf
        }
      } else _df
    } else _df
  }

  private def createStructChangedDf(df: DataFrame, side: String): DataFrame = {
    notJoinBothCols.foldLeft(df) { case (accum, item) =>
      accum.withColumn(item + side, col(item)).drop(item)
    }
  }
}