package ot.scalaotl
package parsers

import ot.scalaotl.extensions.StringExt._

trait WildcardParser {
  case class WcMatch(initial: String, matchstr: String, transformed: String)

  /**
   * Performs replacement in string from input mask to output mask:
   *  - Replaces asterisks in input mask with (.*), escapes meta-characters.
   *  - Does replacement, returns both old and new string.
   * 
   * Example:
   *  - init string = "t1{}.values"
   *  - inputMask = "t*{}.values"
   *  - outputMask = "field*"
   * 
   * Return:
   *  - WcMatch("t1{}.values", "field1")
   * 
   * @param str [[String]] - initial string
   * @param inputMask [[String]] - mask for matching init string, must contains asterisk(s).
   * @param outputMask [[String]] - mask for output string, must contains asterisk(s).
   * 
   * @return init string, string with replacements [[WcMatch]]
   */
  def replaceByMask(str: String, inputMask: String, outputMask: String): Option[WcMatch] = {
    if (inputMask.contains("*") && outputMask.contains("*")) {
      val regexEscapedChars = """<([{\^-=$!|]})?+.>""" //regexp meta-characters w/o asterisk (*)
      val inputMaskSub = "^" + inputMask.escapeChars(regexEscapedChars).replace("*", "(.*)") + "$"
      inputMaskSub.r.findFirstMatchIn(str).map(x => WcMatch(x.matched, x.group(1), outputMask.replace("*", x.group(1))))
    } else {
      if (str == inputMask) Some(WcMatch(inputMask, "", outputMask)) else None
    }
  }

  def replaceByMask(strArr: Array[String], inputMask: String, outputMask: String): Array[WcMatch] = {
    strArr.flatMap(x => replaceByMask(x, inputMask, outputMask))
  }
  
  def returnsWithWc: (Array[String], Return) => Return = (columns: Array[String], returns: Return) => {
    val columnsWithBackticks = columns.map(_.stripBackticks().addSurroundedBackticks)
    val newReturns = returns.fields.flatMap {
      case ReturnField(newfieldMask, fieldMask) => replaceByMask(columnsWithBackticks, fieldMask, newfieldMask)
        .map { case WcMatch(a, _, b) => ReturnField(b.stripBackticks(), a) }
        .sortBy(_.field)
    }

    val newFuncs = returns.funcs.flatMap {
      case StatsFunc(newfieldMask, func, fieldMask) =>
        replaceByMask(columnsWithBackticks, fieldMask, newfieldMask)
          .map { case WcMatch(a, _, b) => StatsFunc(b, func, a) }
          .sortBy(_.field)
    }

    Return(newReturns, newFuncs, returns.evals)
  }

  def getMatches(columns: Array[String], returns: Return): List[(String, String)] = {
    val columnsWithBackticks = columns.map(_.stripBackticks().addSurroundedBackticks)
    val retMatches = returns.fields.flatMap {
      case ReturnField(newfieldMask, fieldMask) => replaceByMask(columnsWithBackticks, fieldMask, newfieldMask)
    }
    val funcMatches = returns.funcs.flatMap {
      case StatsFunc(newFieldMask, _, fieldMask) => replaceByMask(columnsWithBackticks, fieldMask, newFieldMask)
    }
    (retMatches ::: funcMatches).map{
      case WcMatch(initial, matchstr, _) => (initial, matchstr)
    }
  }
}
