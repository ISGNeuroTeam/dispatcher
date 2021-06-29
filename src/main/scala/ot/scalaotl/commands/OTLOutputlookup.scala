package ot.scalaotl
package commands

import java.net.URI

import ot.scalaotl.config.OTLLookups
import ot.scalaotl.extensions.StringExt._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.DataFrame

class OTLOutputlookup(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLLookups with OTLSparkSession {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("append")
  val lookupFileName: String = returns.flatFields.headOption.getOrElse("-1")
  override def fieldsUsed: List[String] = super.fieldsUsed.diff(List(lookupFileName))
  val inputPath: Option[String] = _getLookupPath(lookupFileName.stripBackticks())

  override def transform(_df: DataFrame): DataFrame = {
    inputPath match {
      case Some(path) =>
        val df_w = if (new java.io.File(URI.create(path).getPath).exists()
          & (getKeyword("append").getOrElse("false").toLowerCase == "true")) {
          val df_add = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
          val newCols = df_add.columns.diff(_df.columns)
          val oldCols = _df.columns.diff(df_add.columns)

          newCols.foldLeft(_df)((a, b) => a.withColumn(b, lit(null)))
            .unionByName(oldCols.foldLeft(df_add)((a, b) => a.withColumn(b, lit(null))))
        } else {
          _df
        }
        write(df_w, path)
        df_w
      case _ => _df
    }
  }
}
