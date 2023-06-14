package ot.scalaotl
package extensions

import org.apache.spark.sql.types.NullType

object DataFrameExt {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.NumericType
  import org.apache.spark.sql.{Column, DataFrame}

  implicit class BetterDF(df: DataFrame) {
    // Add and drop '__fake__' column 
    def withFake = df.withColumn("__fake__", lit("__fake__"))
    def dropFake = df.drop("__fake__")

    // Add and drop column with index (row numbers)
    def withIndex(idx: String = "idx") = df.withColumn(idx, monotonically_increasing_id)
    def dropIndex(idx: String = "idx") = df.drop(idx)

    // If column with target name (while renaming) is already existed if df,
    // it might lead to possible fails in future operations.
    // To prevent it, drop possible column with new name before renaming.
    def withSafeColumnRenamed(oldname: String, newname: String) = {
      if (oldname == newname) df
      else df.drop(newname).withColumnRenamed(oldname, newname)
    }

    // Convert _time column from milliseconds to seconds and vice versa.
    // Logic for checking whether conversion needed, implemented in udfs in EvalFunctions.
    // def convertTimeToSec = df.withColumn("_time", EvalFunctions.convertTimeToSec(col("_time")))
    // def convertTimeToMilliSec = df.withColumn("_time", EvalFunctions.convertTimeToMilliSec(col("_time")))

    // Apply multiple aggregations
    def complexAgg(aggFuncs: Column*): DataFrame = aggFuncs.toList match {
      case h :: t => df.agg(h, t: _*)
      case _      => df
    }
    def complexAgg(aggFuncs: List[Column]): DataFrame = aggFuncs match {
      case h :: t => df.agg(h, t: _*)
      case _      => df
    }

    // Apply multiple groupby aggregations
    def complexAgg(group: String, aggFuncs: Column*): DataFrame = {
      aggFuncs.toList match {
        case head :: tail => df.groupBy(group).agg(head, tail: _*)
        case _            => df
      }
    }
    def complexAgg(group: String, aggFuncs: List[Column]): DataFrame = {
      complexAgg(group, aggFuncs: _*)
    }

    def complexAgg(groups: List[String], orderBy: String, aggFuncs: Column*): DataFrame = {
      aggFuncs.toList match {
        case head :: tail => {
          groups match {
            case ghead :: gtail => df.groupBy(ghead, gtail: _*).agg(head, tail: _*).sort(orderBy)
            case _              => df.agg(head, tail: _*).sort(orderBy)
          }
        }
        case _ => df
      }
    }
    def complexAgg(groups: List[String], orderBy: String = "__internal__", aggFuncs: List[Column]): DataFrame = complexAgg(groups, orderBy, aggFuncs: _*)

    /** Collects dataframe as boolean expression.
     * All value in columns combine with "AND" statement.
     * Then all rows combine with "OR" statement.
     * 
     * Example:
     * df = ...
     * +--------+---------+
     * |    name|   family|
     * +--------+---------+
     * |Daenerys|Targaryen|
     * |    John|     Snow|
     * +--------+---------+
     * 
     * df.collectToBooleanExpr
     * String = "(name=\"John\" AND family=\"Snow\") OR (name=\"Daenerys\" AND family=\"Targaryen\")"
     */
    def collectToBooleanExpr = {
      val columns = df.columns
      val types = df.schema.map(x => (x.name, x.dataType)).toMap
      columns.foldLeft(df) {
        case (accum, c) =>
          if(! c.startsWith("$")) { //accum.withColumn(c, col(c.stripPrefix("$")))
            if (types(c).isInstanceOf[NumericType]) {
              accum.withColumn(c, concat_ws("=", lit(c), col(c)))
            } else {
              accum.withColumn(c, concat_ws("=", lit(c), concat(lit("\""), col(c), lit("\""))))
            }
          } else accum
      }
        .withColumn("_return", concat_ws(" AND ", columns.map(col(_)): _*))
        .agg(collect_set("_return").alias("_return"))
        .withColumn("_return", when(size(col("_return")) === 1, array_join(col("_return"), ""))
          .otherwise(concat(lit("("), array_join(col("_return"), ") OR ("), lit(")")))
        )
        .collect
        .head.get(0).toString
    }
    def append(otherDf: DataFrame): DataFrame = {
      val newCols = otherDf.columns.diff(df.columns)
      val oldCols = df.columns.diff(otherDf.columns)
      newCols.foldLeft(df)((a, b) => a.withColumn(b, lit(null)))
        .unionByName(oldCols.foldLeft(otherDf)((a, b) => a.withColumn(b, lit(null))))
    }

    def getColumTypeName(name: String): String ={
      df.schema.toList.filter(_.name == name).map(_.dataType.typeName).headOption.getOrElse("")
    }
    def notNullColumns: List[String] ={
      df.schema.fields.filter(_.dataType!=NullType).map(_.name).toList
    }
  }
}
