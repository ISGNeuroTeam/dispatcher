package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import ot.scalaotl.parsers.ReplaceParser

class OTLDelta(sq: SimpleQuery) extends OTLBaseCommand(sq) with ReplaceParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("p")

  override def transform(_df: DataFrame): DataFrame = {
    val p = getKeyword("p").getOrElse(
      "1").toInt
    returns.fields.foldLeft(_df) {
      case (accum, ReturnField(newfield, field)) =>
        val nf = if (newfield == field) s"delta($field)" else newfield
        //Indexing of dataframe
        var extendedAccum = accum.withColumn("__id__", monotonically_increasing_id()).withColumn("zero", lit(0)).withColumn("p", lit(p + 1))
        //Creating of dataframe with columns whose values contain sliding partitioning
        //Cycle to p - size of delta
        for (i <- 0 to p) {
          //Column(-s): value - current index of cycle
          val indexedAccum = extendedAccum.withColumn("item" + i, lit(i))
          //Added column(-s) of remainder of difference of frame index with cycle index by p
          val remainderedAccum = indexedAccum.withColumn("rm", (col("__id__") - col("item" + i)) % col("p"))
          //Added numeric column(-s), which will using for sliding partitions with delta size and step = 1
          val preWindowedAccum = remainderedAccum.withColumn("n" + i, when(remainderedAccum("rm") < remainderedAccum("zero"), col("__id__"))
            .otherwise(col("__id__") - col("rm")))
          //Created window with sliding partition
          val w = Window.partitionBy("n" + i).orderBy("__id__").rowsBetween(-p, 0)
          //Added column(-s) with sliding partition
          val deltaWindowedAccum = preWindowedAccum.withColumn("d" + i, first(col(field)).over(w))
          extendedAccum = deltaWindowedAccum.drop("n" + i)
          extendedAccum = extendedAccum.sort(List(asc("__id__")): _*)
        }
        //Preparing for delta calcing
        var calcDeltaAccum = extendedAccum.withColumn(nf, lit(0)).withColumn("start_p", lit(p))
        for (i <- p to 0 by -1) {
          //Remainder defining
          val preCalcDeltaWithRemainderAccum = calcDeltaAccum.withColumn("rm", (col("__id__") - col("item" + i)) % col("p"))
          //Column with deductible value created
          val preCalcDeltaWithDeductibleValsAccum = preCalcDeltaWithRemainderAccum.withColumn(nf, when(preCalcDeltaWithRemainderAccum("rm") === preCalcDeltaWithRemainderAccum("start_p"),
            col("d" + i)).when(preCalcDeltaWithRemainderAccum("rm") < preCalcDeltaWithRemainderAccum("zero"), col("d0")).otherwise(col(nf)))
          calcDeltaAccum = preCalcDeltaWithDeductibleValsAccum.drop("d" + i, "item" + i)
        }
        calcDeltaAccum = calcDeltaAccum.drop("rm", "zero", "__id__", "p", "start_p")
        //Delta value calced
        calcDeltaAccum.withColumn(nf, col(field) - col(nf))
    }
  }
}
