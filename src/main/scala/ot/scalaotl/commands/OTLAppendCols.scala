package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ lit, row_number, array, col, when }
import org.apache.spark.sql.expressions.Window

class OTLAppendCols(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("subsearch")
  val cache = sq.cache
  override def transform(_df: DataFrame): DataFrame = {

    val subsearch: String = getKeyword("subsearch").getOrElse("__nosubsearch__")

    if (cache.contains(subsearch)) {
      val subDf: DataFrame = cache(subsearch)
      val sameColumns = _df.columns.intersect(subDf.columns)
      val subColumns = subDf.columns.map(x => if (sameColumns.exists(y => y == x)) x + "_sub" else x)

      val w = Window.orderBy(lit("const"))
      val main = _df.withColumn("id_", row_number().over(w))
      val sub = subDf.toDF(subColumns: _*).withColumn("id_", row_number().over(w))

      val joined = main.join(sub, main("id_") === sub("id_"), "outer")
      val result = sameColumns.foldLeft(joined)((d, colName) => {
        val subColName = colName + "_sub"
        d.withColumn("tempcol", array(col(colName), col(subColName)))
          .withColumn(colName, when(col("tempcol").getItem(0).isNull, col("tempcol").getItem(1))
            .otherwise(col("tempcol").getItem(0)))
          .drop("tempcol")
          .drop(col(subColName))
      }).drop("id_")

      result
    } else _df
  }
}
