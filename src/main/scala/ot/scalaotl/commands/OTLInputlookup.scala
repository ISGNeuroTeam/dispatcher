package ot.scalaotl
package commands

import ot.dispatcher.OTLQuery
import ot.scalaotl.config.OTLLookups
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import ot.scalaotl.utils.logging.StatViewer

import ot.scalaotl.extensions.StringExt._

class OTLInputlookup(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLLookups with OTLSparkSession {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("append")
  val cache = sq.cache
  val inputPath: Option[String] = _getLookupPath(
    returns.fields.headOption match {
      case Some(ReturnField(k, v)) => k
      case _                       => "-1"
    })

  val whereFilter = args.split("where")

  override def transform(_df: DataFrame): DataFrame = {
    log.debug(s"InputPath: $inputPath")
    val inputDf = inputPath match {
      case Some(path) => {
        log.debug(s"Lookup path: $path.")
        var df_add = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        val nullFields = fieldsUsedInFullQuery.diff(List(args)).diff(df_add.schema.names)
        df_add = nullFields.foldLeft(df_add)((acc, fieldName) => acc.withColumn(fieldName, lit(null)))
        log.debug("\n" + StatViewer.getPreviewString(df_add))
        if (getKeyword("append").getOrElse("f") == "t") {
          val newCols = df_add.columns.diff(_df.columns)
          val oldCols = _df.columns.diff(df_add.columns)
          newCols.foldLeft(_df)((a, b) => a.withColumn(b, lit(null))).unionByName(
            oldCols.foldLeft(df_add)((a, b) => a.withColumn(b, lit(null))))
        } else {
          df_add
        }
      }
      case _ => _df
    }

    args.split("where").drop(1) match {
      case Array(f) => {
        val otlQuery = OTLQuery(s"search ${f.mkString("").trim}")
        new Converter(otlQuery, cache).setDF(inputDf).run
      }
      case Array() => inputDf
    }
  }
}
