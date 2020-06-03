package ot.scalaotl
package parsers

case class MatchSubsearch(matched: String, subsearch: String)

trait SubsearchParser extends DefaultParser {
  def parseSubsearch(args: String): Option[MatchSubsearch] = {
    val subsearchRegex = """\[(.*)\]""".r
    subsearchRegex.findFirstMatchIn(args)
      .map(x => MatchSubsearch(x.matched, x.group(1)))            
  }
    
  def getSubsearch = (args: String) => parseSubsearch(args).map(_.subsearch)
    
  override def returnsParser = (args: String, seps: Set[String]) => {
    val matchedSubsearch = parseSubsearch(args).map(_.matched)
    val newArgs = args.replaceAllLiterally(matchedSubsearch.getOrElse(""), "")
    super.returnsParser(newArgs, seps)
  }
}

