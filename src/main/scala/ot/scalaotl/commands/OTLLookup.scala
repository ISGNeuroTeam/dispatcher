package ot.scalaotl
package commands

import ot.scalaotl.parsers.ReplaceParser
import ot.scalaotl.config.OTLLookups
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.extensions.DataFrameExt._

import org.apache.spark.sql.functions.collect_set
import org.apache.spark.sql.DataFrame

class OTLLookup(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("output", "outputnew")) with OTLLookups with ReplaceParser with OTLSparkSession {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]
  val lookupFile: Option[Option[String]] = args.split(" ").headOption.map(_getLookupPath)
  val inputs: Return = args.split(seps.map(_.addExtraSpaces).mkString("|")).toList match {
    case head :: tail => returnsParser(head.split(" ").drop(1).mkString(" "), Set.empty)
    case _            => Return(List[ReturnField]())
  }

  val _list: List[String] = args.split(seps.map(_.addExtraSpaces).mkString("|")).toList
  override val returns: Return = _list match {
    case head :: tail => returnsParser(tail.mkString(""), Set.empty)
    case _            => Return(List(ReturnField("*", "*")))
  }

  override def fieldsUsed: List[String] = inputs.flatNewFields

  override def transform(_df: DataFrame): DataFrame = {
    val dfLookup = lookupFile match {
      case Some(Some(path)) => spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
      case _ => spark.emptyDataFrame
    }

    val _dfCols = _df.columns.toList
    val initInputCols = inputs.fields.map(_.newfield)
    val lookupInputCols = inputs.fields.map(_.field)
    val licNoBckTck = lookupInputCols.map(_.stripBackticks())
    val lookupOutputCols = if (returns.fields.isEmpty) dfLookup.columns.toList.diff(licNoBckTck) else returns.fields.map(_.field)
    val outputCols = if (returns.fields.isEmpty) dfLookup.columns.toList.diff(licNoBckTck) else returns.fields.map(_.newfield)

    if (initInputCols.nonEmpty
      & initInputCols.forall(x => _dfCols.contains(x))
      & licNoBckTck.forall(dfLookup.columns.contains)
      & lookupOutputCols.forall(dfLookup.columns.contains)) {

      val lookupCols = inputs.fields.map(x => ReturnField(x.field.stripBackticks(),x.newfield)) ++ returns.fields
      val totalNewCols = initInputCols ++ outputCols
      val jdf = lookupCols.foldLeft(dfLookup) { case (accum, item) =>
        accum.withSafeColumnRenamed(item.newfield,item.field)
      }
      val jdfSelect = totalNewCols.map(_.addSurroundedBackticks) match {
        case h :: t => jdf.select(h, t: _*)
        case _      => jdf
      }
      val isIntersects = !_df.schema.toList.exists(x => initInputCols.contains(x.name) && x.dataType.typeName == "null")
      if(isIntersects){
      val funcs = outputCols.map(x => collect_set(x).alias(x))
      val dfJoined = _df.join(jdfSelect, initInputCols, "left")
      _dfCols.map(_.addSurroundedBackticks) match {
        case h :: t =>
          funcs match {
            case fh :: ft => dfJoined
              .groupBy(h, t: _*)
              .agg(fh, ft: _*)
            case _ => dfJoined
          }
        case _ => dfJoined
      }
      }else _df

    } else _df
  }
}
