package ot.scalaotl
package parsers

import ot.scalaotl.static.{EvalFunctions, OtHash}
import ot.scalaotl.static.OtHash.md5
import ot.scalaotl.extensions.StringExt._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.catalyst.expressions.Expression

trait ExpressionParser extends DefaultParser {

  def getQuotesReplaceMap(q: String): Map[String, String] = """(["'])(?:(?=(\\?))\2.)*?\1""".r.findAllIn(q).toList.map { x => (x -> md5(x)) }.toMap

  def splitMultipleEvals(evals: String): Array[String] = {

    // Replace all texts between quotes with theirs hashes
    val replMap = getQuotesReplaceMap(evals)
    val evalsWithReplaceQuotes = evals.replaceByMap(replMap)

    // Find all matches for "<varname>="
    val vars = """(^|,)\s*([\$a-zA-Z0-9_]+)\s*=""".r.findAllIn(evalsWithReplaceQuotes).toList

    // Split full string by all matches for "<varname>=". Get corresponding expressions
    val expressions = evalsWithReplaceQuotes
      .split(vars.mkString("|"))
      .filter(_.nonEmpty)

    // Zip varnames and expressions, make query with multiple evals
    vars.map(_.stripPrefix(",").stripSuffix(","))
      .zip(expressions)
      .map { case (x, y) => s"$x$y" }
      .map(_.replaceByMap(replMap.map(_.swap))) // Replace hashes with initial quoted substrings
      .toArray
  }

  override def returnsParser = (args: String, _) =>{
      val caseExpr = findCaseString(args)
      val modfifArgs = if(caseExpr!="") args.replace(caseExpr, OtHash.md5(caseExpr)) else args
    val res=Return(
      fields = List(),
      funcs = List(),
      evals = splitMultipleEvals(modfifArgs).toList.map(
        _.split ("=").toList match {
          case h :: t if t != Nil => StatsEval(h.trim, EvalFunctions.argsReplace (t.mkString ("=").trim))
        }
      ))
      if (caseExpr!="") res.modifyEvalExprs(_.replace(OtHash.md5(caseExpr), caseExpr)) else res
  }

  def findCaseString(str:String) = {
    def getNextChar(str: String, index: Int, unb: Int): String = {
      str.charAt(index) match {
        case '(' => getNextChar(str, index + 1, unb + 1)
        case ')' => if (unb - 1 == 0) str.substring(0, index + 1)
        else getNextChar(str, index + 1, unb - 1)
        case _ => getNextChar(str, index + 1, unb)
      }
    }
    val initialIdx = str.indexOf("case(")
    if( initialIdx != -1) getNextChar(str.substring(initialIdx), 0, 0) else ""
  }


  override def getFieldsUsed = (ret: Return) => {
    ret.evals.flatMap {
      case x if x.expr.isEmpty => List.empty[String]
      case x => getFieldsFromExpression(F.expr(x.expr).expr, List()).map(_.addSurroundedBackticks)
    }
  }

  def getFieldsFromExpression(expr: Expression, fields: List[String]): List[String] = {
    val newfields = if (expr.nodeName == "UnresolvedAttribute") {
      expr.toString.stripPrefix("'") :: fields
    } else fields
    val children = expr.children
    if (children.nonEmpty) {
      children.foldLeft(fields) {
        (accum, item) => accum ++ getFieldsFromExpression(item, newfields)
      }
    } else newfields
  }

  override def getFieldsGenerated = (ret: Return) => ret.evals.map(_.newfield)
}
