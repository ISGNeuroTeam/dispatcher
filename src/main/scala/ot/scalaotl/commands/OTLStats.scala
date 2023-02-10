package ot.scalaotl
package commands

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.{StatsParser, WildcardParser}
import ot.scalaotl.static.StatsFunctions

class OTLStats(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) with StatsParser with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override val fieldsUsed: List[String] = getFieldsUsed(returns) ++ getPositionalFieldUsed(positionals)
  override val fieldsGenerated: List[String] = getFieldsGenerated(returns)

  override def transform(_df: DataFrame): DataFrame = {
    val dd = _df.rdd
    val dLastVal = dd.zipWithUniqueId.map(_.swap).top(1)(Ordering[Long].on(_._1)).headOption.map(_._2)
    val drLastVal = dLastVal.get.getAs[String]("__well_num")

    /*val repartDf = _df.repartition(1).toDF
    val sortList = List("_time").intersect(_df.columns.toList).map(x => x.addSurroundedBackticks).map(x => asc(x))
    val sortedDf = repartDf.sort(sortList: _*)
    val rdd = sortedDf.rdd
    val dCount = rdd.count()
    val parts = rdd.getNumPartitions
    val lastVal = rdd.zipWithUniqueId.map(_.swap).top(1)(Ordering[Long].on(_._1)).headOption.map(_._2)
    val rLastVal = lastVal.get.getAs[String]("__well_num")*/
    // Sort DF if "earliest" or "latest" functions
    val sortNeeded = returns.funcs.map(_.func).intersect(List("earliest", "latest")).nonEmpty
    val workDf = _df.withFake
    //val w = Window.partitionBy("__fake__").orderBy("_time")
    val dfSorted = if (sortNeeded) workDf.orderBy("_time") else workDf

    // Calculate evaluated field. Add __fake__ column for possible 'count' function
    val dfWithEvals = StatsFunctions.calculateEvals(returns.evals, dfSorted)

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
      case _ => StatsFunctions.applyFuncs(rWcFunks, rdfWithEvals)
    }
    val serviceFields = dfCalculated.columns.filter(x => x.matches("__.*__") || x.startsWith("__fake__"))
    val dfView = dfCalculated.collect()
    dfCalculated.dropFake.drop(serviceFields: _*)
  }

  def resolveAmbigousFields(funcs: List[StatsFunc], df: DataFrame): (List[StatsFunc], DataFrame) = {
    val generated_fiedls = funcs.map { case StatsFunc(out, _, in) => (out, in) }.filter(x => x._1 != x._2).map(_._1.stripBackticks()).toSet
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
