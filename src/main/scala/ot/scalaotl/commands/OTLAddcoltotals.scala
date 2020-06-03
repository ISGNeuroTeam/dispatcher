package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.NumericType

class OTLAddcoltotals(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("labelfield", "label")  
  override def transform(_df: DataFrame): DataFrame = {

    val labelfield = getKeyword("labelfield").getOrElse("None")
    val label = getKeyword("label").getOrElse("Total")
    val fieldlist = if (returns.flatFields.isEmpty) _df.columns else returns.flatFields.toArray
    val numericList = _df.schema.filter(_.dataType.isInstanceOf[NumericType])

    if (numericList.isEmpty) {
      _df
    } else {
      val numeric = numericList.map(_.name)
        .toArray
        .filter(fieldlist.contains(_))
      val other = _df.columns.diff(numeric)
      val aggs = numeric.map(name => sum(col(name)).as(name))

      if (labelfield == "None") {
        _df.unionByName(other.foldLeft(_df.agg(aggs.head, aggs.tail: _*))((data, colName) => {
          data.withColumn(colName, lit(null))
        }))
      } else {
        val labeledDf = _df.withColumn(labelfield, lit(null))

        labeledDf.unionByName(other.foldLeft(labeledDf.agg(aggs.head, aggs.tail: _*))((data, colName) => {
          data.withColumn(colName, lit(null))
        }).withColumn(labelfield, lit(label)))
      }
    }
  }
}
