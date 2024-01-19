package ot.scalaotl
package parsers

import ot.scalaotl.commands.OTLEval
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.static.StatsFunctions

import scala.util.matching.Regex

trait StatsParser extends DefaultParser {

  def rexSimpleStatsFunc(funcs: String): Regex = (s"($funcs)" + """(\((\'.+?\'|\S+?)\))?(\s*as\s*([^,\s]+))?(,|\s|$)""").r
  def rexStatsEvalFunc(funcs: String): Regex = (s"($funcs)" + """\(eval\((.*?)\)\)\s*as\s*([a-zA-Zа-яА-Я0-9]+)(\s|,|$)""").r

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

  override def returnsParser: (String, Set[String]) => Return = (args: String, seps: Set[String]) => {
    val argsFiltered = excludeKeywords(excludePositionals(args, seps), keywordsParser(args))    
    val (evals, argsNoEvals) = parseEvals(argsFiltered)    
    Return(fields = List(), parseSimple(argsNoEvals), evals)
  }

  override def getFieldsUsed: Return => List[String] = (ret: Return) => {
    val newEvalFields = ret.evals.map(_.newfield)
    val funcFields = ret.funcs.map(_.field).filterNot(x => newEvalFields.contains(x.stripBackticks()))
    val evalFields = ret.evals
      .map(x => s"${x.newfield} = ${x.expr}").flatMap(x => new OTLEval(SimpleQuery(x)).fieldsUsed)
    (funcFields ++ evalFields).distinct.filterNot(_ == "__fake__")
  }

  override def getFieldsGenerated: Return => List[String] = (ret: Return) => {
    val newEvalFields = ret.evals.map(_.newfield)
    val newFuncFields = ret.funcs.map(_.newfield)
    (newFuncFields ++ newEvalFields).distinct
  }
}
