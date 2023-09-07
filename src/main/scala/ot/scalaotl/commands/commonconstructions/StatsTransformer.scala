package ot.scalaotl.commands.commonconstructions

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{lit, max, min, monotonically_increasing_id}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.{StatsParser, WildcardParser}
import ot.scalaotl.static.StatsFunctions
import ot.scalaotl._

class StatsTransformer(sq: SimpleQuery, context: StatsContext) extends StatsParser with WildcardParser {
  val spark: SparkSession = context.spark
  val returns: Return = context.returns
  val positionals: Map[String, Field] = context.positionalsMap
  //Time column (for defining it's min or max values in time functions). _time by defaut, else other user defined column
  val timeColumn: String = context.timeColumn
  val timeFuncs: List[String] = List("earliest", "latest", "first", "last")
  //Checkings to time or non-time function
  val timeChecking: StatsFunc => Boolean = { f: StatsFunc => timeFuncs.contains(f.func) }
  val nonTimeChecking: StatsFunc => Boolean = { f: StatsFunc => !timeFuncs.contains(f.func) }
  //Lists for separating time and non-time functions from query
  var timeFuncsList: List[List[StatsFunc]] = List()
  var nonTimeFuncsList: List[List[StatsFunc]] = List()

  //Columns for result parts joining to one Dataframe
  val joinedColumns: List[String] = positionals.get("by") match {
    case Some(Positional("by", groups)) if groups.nonEmpty => groups
    case _ => List("__internal__")
  }

  def transform(_df: DataFrame): DataFrame = {
    //Setting for correct join work
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    // Sort DF if "earliest" or "latest" functions
    val sortNeeded = returns.funcs.map(_.func).intersect(timeFuncs).nonEmpty
    val dfSorted = if (sortNeeded) _df.orderBy(timeColumn) else _df
    val dfWithEvals = StatsFunctions.calculateEvals(returns.evals, dfSorted).withFake
    // Replace wildcards with actual column names
    val cols: Array[String] = dfWithEvals.columns
    val returnsWcFuncs: List[StatsFunc] = returnsWithWc(cols, returns).funcs
    val (rWcFunks, rdfWithEvals) = resolveAmbigousFields(returnsWcFuncs, dfWithEvals)
    var aggFuncs = rWcFunks
    //Defining start checking according to functions order in query. Checkings used for separation to time and non-time functions lists.
    val startChecking = if (timeFuncs.contains(aggFuncs.head.func)) {
      timeChecking
    } else {
      nonTimeChecking
    }
    var workChecking = startChecking
    //Lists of functions creating, based on takeWhile(currentChecking), with checkings switchings and non-union of lists, produced by one checking from different steps
    while (aggFuncs.nonEmpty) {
      val confirmingFunctionsBlock: List[StatsFunc] = aggFuncs.takeWhile(workChecking)
      aggFuncs = aggFuncs.dropWhile(workChecking)
      writeToFuncsList(workChecking, confirmingFunctionsBlock)
      workChecking = getInvertChecking(workChecking)
    }
    //Defining of order of separated lists of functions
    val firstFuncsList: List[List[StatsFunc]] = if (startChecking == timeChecking) {
      timeFuncsList
    } else nonTimeFuncsList
    val secondFuncsList: List[List[StatsFunc]] = if (firstFuncsList == timeFuncsList) {
      nonTimeFuncsList
    } else timeFuncsList
    //Creating of dataframes-parts,confirming to functions lists separating
    var dfs: List[DataFrame] = List()
    for ((_, i) <- firstFuncsList.zipWithIndex) {
      dfs = dfs :+ createPartedDf(rdfWithEvals, firstFuncsList(i))
      if (secondFuncsList.length > i) dfs = dfs :+ createPartedDf(rdfWithEvals, secondFuncsList(i))
    }
    //Joining of dataframes-parts and preparing of result
    val dfAll = dfs.tail.foldLeft(dfs.head) { (accum, subElem) =>
      accum.join(subElem, joinedColumns.map(_.stripBackticks))
    }
    val serviceFields = dfAll.columns.filter(x => x == "__internal__" || x.startsWith("__fake__") || x.matches("__.*__"))
    dfAll.drop(serviceFields: _*)
  }

  /**
   * Write block of functions to confirming list
   *
   * @param checking
   * @param confirmingFunctionsBlock
   */
  private def writeToFuncsList(checking: StatsFunc => Boolean, confirmingFunctionsBlock: List[StatsFunc]): Unit = {
    if (checking == timeChecking) {
      timeFuncsList = timeFuncsList :+ confirmingFunctionsBlock
    } else {
      nonTimeFuncsList = nonTimeFuncsList :+ confirmingFunctionsBlock
    }
  }

  /**
   * get another checking than input parameter (from 1 alternative)
   *
   * @param checking
   * @return
   */
  private def getInvertChecking(checking: StatsFunc => Boolean): StatsFunc => Boolean = {
    if (checking == timeChecking) {
      nonTimeChecking
    } else {
      timeChecking
    }
  }

  /**
   * resolving of ambigoud fields
   *
   * @param funcs funcs list
   * @param df    dataframe
   * @return
   */
  private def resolveAmbigousFields(funcs: List[StatsFunc], df: DataFrame): (List[StatsFunc], DataFrame) = {
    val generated_fields = funcs.map { case StatsFunc(out, _, in) => (out, in) }.filter(x => x._1 != x._2).map(_._1.stripBackticks()).toSet
    val inputCols = df.columns.toSet
    val ambigousFields = inputCols.intersect(generated_fields)
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

  /**
   * Created dataframe based on stats logic with functions from input list
   *
   * @param df        started dataframe
   * @param funcsList list of time or non-time functions
   * @return
   */
  private def createPartedDf(df: DataFrame, funcsList: List[StatsFunc]): DataFrame = {
    if (timeFuncsList.contains(funcsList)) {
      val timedDfs: List[DataFrame] = for (func <- funcsList)
        yield calcTimeFuncs(df, func)
      timedDfs.tail.foldLeft(timedDfs.head) { (accum, subElem) =>
        accum.join(subElem, joinedColumns.map(_.stripBackticks()))
      }
    } else {
      calcFuncs(df, funcsList)
    }
  }

  /**
   * Dataframe transforming by some time function
   *
   * @param df
   * @param func
   * @return
   */
  private def calcTimeFuncs(df: DataFrame, func: StatsFunc): DataFrame = {
    val isFirst = (func.func == "earliest" || func.func == "first")
    val dfWithInternal = df.withColumn("__internal__", lit(0))
    val workDf = dfWithInternal.repartition(1)
    val dfIndexed: DataFrame = workDf.withColumn("__index__", monotonically_increasing_id())
    val winPartition = joinedColumns.head.stripBackticks()
    //Defining target row using min/max time value and window
    val window: WindowSpec = Window.partitionBy(winPartition)
    val dfWithTimeMax = dfIndexed.withColumn("__bndrVal__",
      (if (isFirst) {
        min(timeColumn)
      } else {
        max(timeColumn)
      }).over(window).alias("__bndr__"))
    val dfTarget = dfWithTimeMax.filter(s""" $timeColumn == __bndr__ """).sort("__index__").drop("__bndrVal__")
    calcFuncs(dfTarget, List(func))
  }

  /**
   * Dataframe transforming by some non-time function
   *
   * @param df
   * @param funcs
   * @return
   */
  private def calcFuncs(df: DataFrame, funcs: List[StatsFunc]): DataFrame = {
    val dfWithInternal = df.withColumn("__internal__", lit(0))
    val workFuncs = List(StatsFunc("__internal__", "min", "__internal__")) ::: funcs
    // Calculate aggregations
    positionals.get("by") match {
      case Some(Positional("by", groups)) =>
        var nullColumnFlag = false
        groups.foreach(column => if (dfWithInternal.getColumTypeName(column.stripBackticks()) == "null") nullColumnFlag = true)
        if (nullColumnFlag) {
          val emptyDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
            StructType(Seq(StructField("_raw", StringType))
            ))
          emptyDF
        } else {
          StatsFunctions.applyFuncs(workFuncs, dfWithInternal, groups)
        }
      case _ => StatsFunctions.applyFuncs(workFuncs, dfWithInternal)
    }
  }
}

case class StatsContext(spark: SparkSession, returns: Return, positionalsMap: Map[String, Field], timeColumn: String)
