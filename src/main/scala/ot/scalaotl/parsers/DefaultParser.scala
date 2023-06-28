package ot.scalaotl
package parsers

import ot.scalaotl.extensions.StringExt._

import scala.util.matching.Regex

trait DefaultParser {

  /** Parses key=value pairs from command args.
   * 
   * Example:
   * timechart span=5min limit=0 max(val) as maxval by host
   * 
   * Keywords -> List(Keyword("span", "5min"), Keyword("limit", "0"))
   * 
   * @param args [[String]] - args string
   * @return List[[Keyword]] - list of keyword pairs
   */
  def keywordsParser: String => List[Keyword] = (args: String) => {
    """(\S+)\s*=\s*(".*?"|\S+)""".r.findAllIn(args)
      .matchData
      .map(x => 
        Keyword(
          key = x.group(1),
          value = x.group(2)
        )
      )
      .toList
  }

  def fieldsToMap: Seq[Field] => Map[String, Field] = (vs: Seq[Field]) => vs.flatMap(x => x.toMap).toMap

  /** Parses positional arguments. Positionals are the arguments following separators specified in class instance.
   * 
   * Example:
   * stats min(value) as minval by host, ip
   * 
   * Separators -> Set("by")
   * Positionals -> Positional("by", List("host", "ip"))
   * 
   * @param args [[String]] - args string
   * @param seps [[Set[String]]] - set of separators
   * @return [[Seq[Positional]]] - sequence of positionals
   */
  def positionalsParser: (String, Set[String]) => Seq[Positional] = (args: String, seps: Set[String]) => {
    val sepsExtraSpaces = seps.map(_.addExtraSpaces)
    sepsExtraSpaces.map(
      x => if (args.contains(x)) Positional(
        x.removeExtraSpaces,
        args.split(x)(1)
          .split(sepsExtraSpaces.mkString("|"))
          .head
          .trim
          .split("[ ,]")
          .filter(_.nonEmpty)
          .map(_.strip("'").addSurroundedBackticks)
          .toList
      )
      else Positional(x.removeExtraSpaces, List.empty[String])
    ).toSeq
  }

  /** Removes positionals and separators from arg string
   */
  def excludePositionals: (String, Set[String]) => String = (args: String, seps: Set[String]) => if (seps.isEmpty) args else args.split(seps.mkString("|")).head

  /** Remove one specified keyword pair from arg string
   */
  def excludeOneKeyword: (String, Keyword) => String = (args: String, kw: Keyword) => args.replaceAllLiterally(s"${kw.key}=${kw.value}", "").removeExtraSpaces

  /** Removes all keyword pairs from arg string
   */
  def excludeKeywords: (String, List[Keyword]) => String = (args: String, kws: List[Keyword]) => kws.foldLeft(args) { case (accum, item) => excludeOneKeyword(accum, item) }

  /** Parses fields directly used in calculating and field names to return.
   * Also parses applied functions and eval expressions within arg string.
   * 
   * ---
   * Example 1:
   * stats min(val) as minval, max(eval(if(val > 100000, val , val * 2))) 
   * 
   * Return = Return(
   * List(),
   * List(StatsFunc("minval", "min", "val"), StatsFunc("maxval", "max", "maxval")),
   * List(StatsEval("maxval", "if(val > 100000, val , val * 2)"))
   * )
   * 
   * Example 2:
   * rename val as newval
   * 
   * Return = Return(
   * List(ReturnField("value", "newval")),
   * List(),
   * List())
   * ---
   * 
   * @param args [[String]] - arg string
   * @param seps [[Set[String]]] - set of separators
   * @return `Return` type contains of 3 attributes:
   * - [[ReturnField]] represents simple case with field used in transformation and new field name after transformation
   * - [[List[StatsFunc]]] represents statistical functions applied to fields and new field names after transformation
   * - [[List[StatsEval]]] represents eval expressions calculated over dataframe fields before statistical transformations
   */
  def returnsParser: (String, Set[String]) => Return = (args: String, seps: Set[String]) => {
    val argsFiltered = excludeKeywords(excludePositionals(args, seps), keywordsParser(args)) // TODO Make chain
    val splitSpaceCommaKeepQuotes: Regex = """(?:".*?"|[^,\s])+""".r
    Return(splitSpaceCommaKeepQuotes
      .findAllIn(argsFiltered)
      .map(_.strip("'"))
      .map(x => ReturnField(x.strip("'"), x.addSurroundedBackticks))
      .toList)
  }

  /** Returns list of fields used in command
   */
  def getFieldsUsed: Return => List[String] = (ret: Return) => ret.flatFields

  /** Returns list of new fields created afer transformation
   */
  def getFieldsGenerated: Return => List[String] = (ret: Return) => List[String]()

  /** Returns list of positionals used in command
   */
  def getPositionalFieldUsed: Seq[Positional] => List[String] = (pos: Seq[Positional]) => pos.map { case Positional(_, v) => v }.toList.flatten
}
