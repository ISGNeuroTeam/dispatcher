package ot.scalaotl
package parsers

import scala.util.matching.Regex

import ot.scalaotl.extensions.StringExt._

trait RenameParser extends DefaultParser {

  override def getFieldsUsed = (ret: Return) => {
    ret.fields.flatMap(t => List(t.field, t.newfield)).distinct
  }

  override def returnsParser = (args: String, seps: Set[String]) => {
    val argsFiltered = excludeKeywords(excludePositionals(args, seps), keywordsParser(args))
    /**
     * The following regex just accepts all possible ways to write an argument:
     * "asd" as "bsd"
     * asd as "bsd"
     * "asd" as bsd
     * "as\"d" as "\tsd"
     * te*st as test*
     * Nothing special, just don't touch it :)
     */
    val rexAsWith = """((['"])(?:(?=(\\?))\3.)*?\2|[^\s,]+)\s+(as|with)\s+((['\s"])(?:(?=(\\?))\7.)*?\6|[^\s,]+)(\s|,|$)""".r
    val fields = rexAsWith
      .findAllIn(argsFiltered)
      .matchData
      .map(x => {
        val g1 = StringContext.treatEscapes(x.group(1).withQuotesTrimmed)
        val g5 = StringContext.treatEscapes(x.group(5).withQuotesTrimmed)
        ReturnField(Option(g5).getOrElse(g1), g1)
      })
      .map{ case ReturnField(nf, f) =>
        ReturnField(
        nf.replace("&ph", " ").strip("'"),
        f.replace("&ph", " ").strip("'").addSurroundedBackticks)}
      .toList
    Return(fields)
  }
}