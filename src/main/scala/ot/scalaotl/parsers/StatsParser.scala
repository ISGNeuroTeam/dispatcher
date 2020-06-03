package ot.scalaotl
package parsers

import ot.scalaotl.static.StatsFunctions
import ot.scalaotl.commands.OTLEval
import ot.scalaotl.extensions.StringExt._

trait StatsParser extends DefaultParser {

  def rexSimpleStatsFunc(funcs: String) = (s"($funcs)" + """(\((\'.+?\'|\S+?)\))?( as ([^,\s]+))?(,|\s|$)""").r
  def rexStatsEvalFunc(funcs: String) = (s"($funcs)" + """\(eval\((.*?)\)\) as ([a-zA-Z0-9]+)(\s|,|$)""").r

  def parseEvals(args: String, funclist: String = StatsFunctions.funclistString): (List[StatsEval], String) = {
    val evals = rexStatsEvalFunc(funclist).findAllIn(args)
      .matchData
      .map(x => StatsEval(x.group(3), x.group(2)))
      .toList
    val newcmd = evals.foldLeft(args) {
      case (accum, item) => accum.replaceAllLiterally(s"eval(${item.expr})", item.newfield.strip("'"))
    }
    (evals, newcmd)
  }

  def parseSimple(args: String, funclist: String = StatsFunctions.funclistString): List[StatsFunc] = {
    def innerParse(s: String): List[StatsFunc] = {
      rexSimpleStatsFunc(funclist).findAllIn(s).matchData.map(x => {
        val oldf = Option(x.group(3)).getOrElse("__fake__").trim.strip("'")
        val func = x.group(1).trim
        val newf = Option(x.group(5)).getOrElse(if (func == "count") "count" else s"$func($oldf)").trim
        StatsFunc(newf.strip("'"), func, oldf)
      }).toList
    }
    
    args.withKeepQuotedText[List[StatsFunc]](innerParse).map(
      x => StatsFunc(x.newfield.strip("\"").addSurroundedBackticks, x.func, x.field.addSurroundedBackticks)
    )
  }

  override def returnsParser = (args: String, seps: Set[String]) => {
    val argsFiltered = excludeKeywords(excludePositionals(args, seps), keywordsParser(args))    
    val (evals, argsNoEvals) = parseEvals(argsFiltered)    
    Return(fields = List(), parseSimple(argsNoEvals), evals)
  }

  override def getFieldsUsed = (ret: Return) => {
    val newEvalFields = ret.evals.map(_.newfield)
    val funcFields = ret.funcs.map(_.field).filterNot(x => newEvalFields.contains(x.toString.stripBackticks))
    val evalFields = ret.evals
      .map(x => s"${x.newfield} = ${x.expr}")
      .map(x => new OTLEval(SimpleQuery(x)).fieldsUsed).flatten
    (funcFields ++ evalFields).distinct.filterNot(_ == "__fake__")
  }

  override def getFieldsGenerated = (ret: Return) => {
    val newEvalFields = ret.evals.map(_.newfield)
    val newFuncFields = ret.funcs.map(_.newfield)
    (newFuncFields ++ newEvalFields).distinct
  }
}
