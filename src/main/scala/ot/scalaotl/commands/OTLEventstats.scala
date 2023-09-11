package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import ot.scalaotl.commands.commonconstructions.{StatsContext, StatsTransformer}
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.StatsParser

class OTLEventstats(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) with StatsParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  val transformer = new StatsTransformer(Left(StatsContext(returns, positionalsMap, "_time")), spark)
  override val fieldsUsed: List[String] = getFieldsUsed(returns) ++ getPositionalFieldUsed(positionals)
  override val fieldsGenerated: List[String] = getFieldsGenerated(returns).map(_.stripBackticks())

  override def transform(_df: DataFrame): DataFrame = {
    val outDf = transformer.transform(_df)
    if (!outDf.isEmpty) {
      val workDf = _df.drop(fieldsGenerated: _*)
      val eventstatsDf = (positionalsMap.get("by") match {
        case Some(Positional("by", List())) => workDf.crossJoin(outDf)
        case Some(Positional("by", byList)) => {
          val workByList = byList.map(_.stripBackticks())
          val colItem = workDf(workByList.head) <=> outDf(workByList.head)
          val cols = workByList.tail.foldLeft(colItem) {
            (col, p) => col && workDf(p) <=> outDf(p)
          }
          val outColItem = outDf(workByList.head)
          val outCols = workByList.tail.foldLeft(outColItem) {
            (outCol, p) => outCol && outDf(p)
          }
          workDf.join(outDf, cols).drop(outCols)
        }
        case _ => outDf
      }).dropFake
      if (eventstatsDf.columns.contains("_time"))
        eventstatsDf.sort("_time")
      else
        eventstatsDf
    }
    else
      _df
  }
}