package ot.scalaotl
package commands

import ot.scalaotl.static.OtDatetime
import ot.scalaotl.static.StatsFunctions
import ot.scalaotl.parsers.{StatsParser, WildcardParser}
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.StringExt._

import ot.dispatcher.sdk.core.CustomException.E00021

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import scala.util.{Failure, Success, Try}

case class Stream(window: WindowSpec, df: DataFrame)

class OTLStreamstats(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) with StatsParser with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("time_window","window","center")

  override val fieldsUsed: List[String] = getFieldsUsed(returns) ++ getPositionalFieldUsed(positionals)
  override val fieldsGenerated: List[String] = getFieldsGenerated(returns)
  val center: Boolean = Try(getKeyword("center").getOrElse("false").toLowerCase()
    .replaceAll("^t$", "true")
    .replaceAll("^f$", "false").toBoolean) match {
    case Success(v) => v
    case Failure(v) => throw E00021(sq.searchId)
  }

  def getTimeWindow(kw: Map[String, Field], df: DataFrame): Option[Stream] = {
    kw.get("time_window").map {
      case Keyword(k, tw) =>
        val wSize = OtDatetime.getSpanInSeconds(tw)
        val range: (Long, Long) = if (center) (1 - wSize - (Window.currentRow/2 - 1), Window.currentRow/2 + 1)  else (1 - wSize, Window.currentRow)
        val win = Window.orderBy("_time").rangeBetween(range._1, range._2)
        Stream(win, df)
    }
  }

  def getSimpleWindow(kw: Map[String, Field], df: DataFrame): Option[Stream] = {
    kw.get("window").map {
      case Keyword(k, w) =>
        val win = if (w == "global") {
          Window.orderBy("idx").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        } else {
          val wSize = w.toInt
          val range: (Long, Long) = if (center) (1 - wSize - (Window.currentRow/2 - 1), Window.currentRow/2 + 1)  else (1 - wSize, Window.currentRow)
          Window.orderBy("idx").rowsBetween(range._1, range._2)
        }
        val dfout = df.withIndex()
        Stream(win, dfout)
    }
  }

  def getDefaultWindow(df: DataFrame): Stream = {
    val win = Window.orderBy("idx").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    Stream(win, df.withIndex())
  }

  override def transform(_df: DataFrame): DataFrame = {

    // Calculate evaluated fields. Add '__fake__' column for possible 'count' function
    val dfWithEvals = StatsFunctions.calculateEvals(returns.evals, _df).withFake

    // Compose window and modify DF, if needed
    val stream = getTimeWindow(keywordsMap, dfWithEvals).getOrElse(
      getSimpleWindow(keywordsMap, dfWithEvals).getOrElse(
        getDefaultWindow(dfWithEvals)
      )
    )

    // Add partition to window, if needed
    val streamGrouped = positionalsMap.get("by") match {
      case Some(Positional("by", posHead :: posTail)) =>
        Stream(stream.window.partitionBy(posHead, posTail: _*), stream.df)
      case _ => stream
    }

    // Replace wildcards with actual column names
    val cols: Array[String] = dfWithEvals.columns
    val returnsWcFuncs: List[StatsFunc] = returnsWithWc(cols, returns).funcs

    // Apply functions over window
    returnsWcFuncs.foldLeft(streamGrouped.df) {
      case (accum, sf) => accum.withColumn(
        sf.newfield.stripBackticks(),
        StatsFunctions.getExpr(sf.func, sf.field).over(streamGrouped.window)
      )
    }
      .dropFake
      .dropIndex()
  }
}
