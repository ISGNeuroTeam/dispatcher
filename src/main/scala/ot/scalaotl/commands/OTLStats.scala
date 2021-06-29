package ot.scalaotl
package commands

import org.apache.spark.sql.types.{NullType, StringType, StructField, StructType}
import ot.scalaotl.parsers.{StatsParser, WildcardParser}
import ot.scalaotl.static.StatsFunctions
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.StringExt._
import org.apache.spark.sql.{DataFrame, Row}

class OTLStats(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) with StatsParser with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  
  override val fieldsUsed: List[String] = getFieldsUsed(returns) ++ getPositionalFieldUsed(positionals)
  override val fieldsGenerated: List[String] = getFieldsGenerated(returns)

  override def transform(_df: DataFrame): DataFrame = {
    // Sort DF if "earliest" or "latest" functions
    val sortNeeded = returns.funcs.map(_.func).intersect(List("earliest", "latest")).nonEmpty
    val dfSorted = if (sortNeeded) _df.orderBy("_time") else _df

    // Calculate evaluated field. Add __fake__ column for possible 'count' function
    val dfWithEvals = StatsFunctions.calculateEvals(returns.evals, dfSorted).withFake

    // Replace wildcards with actual column names
    val cols: Array[String] = dfWithEvals.columns
    val returnsWcFuncs: List[StatsFunc] = returnsWithWc(cols, returns).funcs
    val (rWcFunks, rdfWithEvals) = resolveAmbigousFields(returnsWcFuncs, dfWithEvals)
    // Calculate aggregations
    val dfCalculated = positionalsMap.get("by") match {
      case Some(Positional("by", groups)) =>
        var nullColumnFlag = false
        groups.foreach(column => if (dfWithEvals.getColumTypeName(column.stripBackticks()) == "null") nullColumnFlag = true)
        if (nullColumnFlag) {
          val emptyDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
            StructType(Seq(StructField("_raw", StringType))
//              ++ fieldsUsed.filterNot(_ == "`__fake__`").map(f => StructField(f.stripBackticks(), NullType))
//              ++ fieldsGenerated.map(f => StructField(f.stripBackticks(), NullType))
            ))
          emptyDF
        } else {
          StatsFunctions.applyFuncs(rWcFunks, rdfWithEvals, groups)
        } //.map(x => "`" + x + "`"))
      case _                              => StatsFunctions.applyFuncs(rWcFunks, rdfWithEvals)
    }
    val serviceFields = dfCalculated.columns.filter(x => x.matches("__.*__") || x.startsWith("__fake__"))
    dfCalculated.dropFake.drop(serviceFields: _*)
  }

  def resolveAmbigousFields(funcs: List[StatsFunc], df: DataFrame): (List[StatsFunc], DataFrame) = {
    val generated_fiedls= funcs.map{case StatsFunc(out,_,in) => (out,in)}.filter(x => x._1 != x._2).map(_._1.stripBackticks()).toSet
    val inputCols = df.columns.toSet
    val ambigousFields = inputCols.intersect(generated_fiedls)
    val aliases = ambigousFields.map(f => (f, s"__${f}__"))
    val mdf = aliases.foldLeft(df)((acc, f) => acc.withSafeColumnRenamed(f._1, f._2))
    val nfuncs = funcs.map {
      case StatsFunc(out, func, in) if ambigousFields.contains(in.stripBackticks()) => if (out == in)
        StatsFunc(s"`__${in.stripBackticks()}__`", func, s"`__${in.stripBackticks()}__`")
      else
        StatsFunc(out, func, s"`__${in.stripBackticks()}__`")
      case x => x
    }
    (nfuncs, mdf)
  }
}
