package ot.scalaotl
package commands

import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.extensions.DataFrameExt._

import org.apache.spark.sql.DataFrame

class OTLJoin(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("subsearch","max","type")
  val cachedDFs: Map[String, DataFrame] = sq.cache
  override def transform(_df: DataFrame): DataFrame = {
    val subsearch: String = getKeyword("subsearch").getOrElse("__nosubsearch__")
    if (cachedDFs.contains(subsearch) && returns.flatFields.nonEmpty) {
      val jdf: DataFrame = cachedDFs(subsearch)
      val joinOn = returns.flatFields.map(_.stripBackticks())
      val bothCols = _df.columns.intersect(jdf.columns)
      log.debug(f"[SearchId:${sq.searchId}] Columns in right df: ${jdf.columns.mkString(", ")}")
      log.debug(f"[SearchId:${sq.searchId}] Columns join on: ${joinOn.mkString(", ")}")
      log.debug(f"[SearchId:${sq.searchId}] Columns in both dashboards: ${bothCols.mkString(", ")}")
      if (joinOn.forall(bothCols.contains)) {
        val dfSuffix = if (jdf.isEmpty) _df //TODO find more convenient vay to check if df is empty
        else bothCols.diff(joinOn).foldLeft(_df) { case (accum, item) =>
          accum.drop(item)}
        val intdf = dfSuffix.join(jdf, joinOn, getKeyword("type").getOrElse("left"))
        if (getKeyword("max").getOrElse("0") == "1") {
          intdf.dropDuplicates(joinOn)
        } else intdf
      } else _df
    } else _df
  }
}
