package ot.scalaotl
package extensions

object RegexExt {
  import scala.util.matching.Regex

  implicit class BetterRegex(r: Regex) {
    def getAllGroups(s: String): List[String] = r.findAllIn(s).matchData.map(x => x.group(1)).toList
  }
}
