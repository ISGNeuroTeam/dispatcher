package ot.scalaotl
package commands

import com.isgneuro.otl.processors.Stats
import com.isgneuro.otl.processors.commonconstructions.StatsContext
import org.apache.spark.sql.DataFrame
import ot.scalaotl.parsers.{StatsParser, WildcardParser}

class OTLStats(sq: SimpleQuery) extends OTLBaseCommand(sq, _seps = Set("by")) with StatsParser with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  override val fieldsUsed: List[String] = getFieldsUsed(returns) ++ getPositionalFieldUsed(positionals)
  override val fieldsGenerated: List[String] = getFieldsGenerated(returns)

  override def transform(_df: DataFrame): DataFrame = {
    val statsFuncs = for (f <- returns.funcs)
      yield com.isgneuro.scalaotl.core.StatsFunc(f.newfield, f.func, f.field)
    val evals = returns.evals.map(e => (e.newfield -> e.expr)).toMap
    val groupFields = positionalsMap get "by" match {
      case Some(Positional("by", groups)) => groups
      case _ => List()
    }
    val statsContext = StatsContext(statsFuncs, evals, groupFields, getKeyword("timeCol").getOrElse("_time"))
    val stats = new Stats(spark, statsContext)
    stats.transform(_df)
    /*val statsTransformer = new StatsTransformer(Left(StatsContext(returns, positionalsMap, getKeyword("timeCol").getOrElse("_time"))), spark)
    statsTransformer.transform(_df)*/
  }

}
