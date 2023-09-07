package ot.scalaotl
package parsers

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{functions => F}
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.static.OtHash.md5
import ot.scalaotl.static.{EvalFunctions, OtHash}

trait ExpressionParser extends DefaultParser {

  def getQuotesReplaceMap(q: String): Map[String, String] = """(["'])(?:(?=(\\?))\2.)*?\1""".r.findAllIn(q).toList.map { x => (x -> md5(x)) }.toMap

  def splitMultipleEvals(evals: String): Array[String] = {
    val evalEqualContFuncs = List("case", "coalesce", "if", "like", "match", "nullif", "tonumber", "tostring", "md5", "sha1", "relative_time", "strptime",
      "isnotnull", "isnull", "abs", "ceiling", "ceil", "exp", "floor", "ln", "log", "pow", "round", "sqrt", "mvappend", "mvcount", "mvdedup", "mvfind",
      "mvindex", "mvjoin", "mvrange", "mvsort", "mvzip", "split", "max", "min", "len", "lower", "replace", "substr", "upper", "acos", "asin", "atan",
      "atan2", "cos", "hypot", "sin", "tan")
    // Replace all texts between quotes with theirs hashes
    val replMap = getQuotesReplaceMap(evals)
    val evalsWithReplaceQuotes = evals.replaceByMap(replMap)
    //Map for replacing eval funcs with <func_name+number>
    val replaceFuncsMap =  evalEqualContFuncs.filter(evalsWithReplaceQuotes.contains(_))
      //Get list of func names and counts of them in expression
      .map(func => {
        var count = 0
        var curEvalTextPart = evalsWithReplaceQuotes
        var funcIndex = curEvalTextPart.indexOf(func)
        while (funcIndex >= 0) {
          curEvalTextPart = curEvalTextPart.substring(funcIndex + func.length)
          funcIndex = curEvalTextPart.indexOf(func)
          count += 1
        }
        (func, count)
      })
      //Build Map(func -> funcReplacement)
      .flatMap(f => {
        //Value of n on last iteration (require for difference between n and lastN calcing for confirming func in expression search)
      var lastN = -1
        //Expression text part in iterations in filter and map for access to text of current function as first function in text
      var iterEvalText = evalsWithReplaceQuotes.substring(evalsWithReplaceQuotes.indexOf(f._1))
        //Filter for correct interval between func name and opened bracket (empty or backspaces)
      (0 until f._2).filter(n => {
        val firstOpenBracketIndex = iterEvalText.indexOf("(")
        if (firstOpenBracketIndex > 0) {
          val funcToOpenBracketPart = if (firstOpenBracketIndex == f._1.length) ""
          else iterEvalText.substring(f._1.length, firstOpenBracketIndex - 1)
          val lastIndex = f._2 - 1
          iterEvalText = if (n == lastIndex) {
            evalsWithReplaceQuotes
          } else {
            val ostText = iterEvalText.substring(iterEvalText.indexOf(f._1) + f._1.length)
            ostText.substring(ostText.indexOf(f._1))
          }
          !funcToOpenBracketPart.exists(_ != ' ')
        } else false
      }).map(n => {
        //Text, started from current func name (by number), created in cycle
        var funcFullPart = iterEvalText
        for (t <- 0 until (n-lastN)) {
          funcFullPart = funcFullPart.substring(funcFullPart.indexOf(f._1))
          if (t != (n-lastN-1)) {
            val funcLength = f._1.length
            funcFullPart = if (funcFullPart.length == funcLength) "" else funcFullPart.substring(funcLength)
          }
        }
        //Function end symbol number defining by opened-closed brackets confirming
        var openBracketsCount = 0
        var closeBracketsCount = 0
        val endTextNum = funcFullPart.length
        var endNum = endTextNum
        for ((symb, i) <- funcFullPart.zipWithIndex) {
          if (openBracketsCount == 0 || openBracketsCount != closeBracketsCount) {
            if (symb == '(')
              openBracketsCount += 1
            if (symb == ')') {
              closeBracketsCount += 1
              if (closeBracketsCount == openBracketsCount)
                endNum = i + 1
            }
          }
        }
        //Next iteration text - without current function
        iterEvalText = if (endNum == endTextNum) "" else funcFullPart.substring(endNum)
        lastN = n
        //Iteration result - map element (function text -> replacement text)
        funcFullPart.substring(0, endNum) -> (f._1 + n.toString)
      })
    }).toMap

    val funcReplacedEvals = evalsWithReplaceQuotes.replaceByMap(replaceFuncsMap)
    // Find all matches for "<varname>="
    val vars = """(^|,)\s*([\$a-zA-Z0-9_]+)\s*=""".r.findAllIn(evalsWithReplaceQuotes).toList

    // Split full string by all matches for "<varname>=". Get corresponding expressions
    val expressionsWithFuncsReplaces = funcReplacedEvals
      .split(vars.mkString("|"))
      .filter(_.nonEmpty)
    val invertedReplaceFuncsMap = (for((k, v) <- replaceFuncsMap) yield (v, k))
    val expressions = for {expr <- expressionsWithFuncsReplaces}
      yield expr.replaceByMap(invertedReplaceFuncsMap)

    // Zip varnames and expressions, make query with multiple evals
    val res = vars.map(_.stripPrefix(",").stripSuffix(","))
      .zip(expressions)
      .map { case (x, y) => s"$x$y" }
      .map(_.replaceByMap(replMap.map(_.swap))) // Replace hashes with initial quoted substrings
      .toArray
    res
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
