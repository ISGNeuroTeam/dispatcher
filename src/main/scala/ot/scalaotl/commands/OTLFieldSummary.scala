package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/** =Abstract=
 * This class provides support of __'''fieldsummary'''__ otl command.
 *
 * __'''fieldsummary'''__ calculates summary statistics on all fields
 *
 * and returns the result as a table.
 *
 * __'''fieldsummary'''__ takes one optional argument '''column_name''' -
 *
 * the name of the field that stores the values of the field names,
 *
 * by default '''column_name''' = '''"column"'''.
 *
 * =Usage example=
 * OTL:
 * {{{| makeresults | eval temperature = mvappend(0.25, 1, 500, 100, 0.002, 23.5)
 *| mvexpand temperature | fields temperature | fieldsummary column_name = name}}}
 * Result:
 * {{{+-----------+-----+-------+-----------+-----+------------------+
|       name|count|    max|       mean|  min|            stddev|
+-----------+-----+-------+-----------+-----+------------------+
|temperature|    6|500.000|104.1253333|0.002|197.73760504938525|
+-----------+-----+-------+-----------+-----+------------------+}}}
 *
 * @constructor creates new instance of [[OTLDelta]]
 * @param sq [[SimpleQuery]]
 */
class OTLFieldSummary(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  override val fieldsGenerated = List("summary")

  def zipUdf: UserDefinedFunction = udf((cols: Seq[String], vals: Seq[String]) => cols.zip(vals))

  /**
   *
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return new dataframe with summary statistics
   */
  override def transform(_df: DataFrame): DataFrame = {

    val describedDF = _df.describe()
    val columnName = getKeyword("column_name").getOrElse("column")
    val columns = describedDF.columns

    describedDF
      .select(col("summary"), zipUdf(lit(columns), array(columns.map(col): _*)).as("temp"))
      .withColumn("rows", explode(col("temp")))
      .select(col("summary"), col("rows._1").as(columnName), col("rows._2").as("value"))
      .groupBy(columnName)
      .pivot("summary")
      .agg(first("value"))
      .where(!(col(columnName) === "summary"))
  }
}