package ot.scalaotl
package parsers

import ot.scalaotl.extensions.StringExt._

import scala.util.matching.Regex

trait ReplaceParser extends DefaultParser {

  override def returnsParser = (args: String, seps: Set[String]) => {
    val argsFiltered = excludeKeywords(excludePositionals(args, seps), keywordsParser(args)).replace("""\"""", "`")
    val splitSpaceCommaKeepQuotes: Regex = """(?:".*?"|[^,\s])+""".r
    val repl = splitSpaceCommaKeepQuotes
      .findAllIn(argsFiltered)
      .map(x => x.replace(" ", "&ph"))
      .toList
      .mkString(" ")
    val rexAsWith = """(\S+)(\s+(as|with)\s+(\S+))?(\s|,|$)""".r
    val fields = rexAsWith
      .findAllIn(repl)
      .matchData
      .map(x => ReturnField(Option(x.group(4)).getOrElse(x.group(1)), x.group(1)))
      .map{ case ReturnField(nf, f) => ReturnField(
        nf.replace("&ph", " ").replace("`", """\"""").strip("'"),
        f.replace("&ph", " ").replace("`", """\"""").strip("'").addSurroundedBackticks)
      }
      .toList
    Return(fields)
  }
}