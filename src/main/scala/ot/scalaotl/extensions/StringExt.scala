package ot.scalaotl
package extensions

import ot.scalaotl.static.OtHash
import scala.util.{ Try, Success, Failure }
import reflect.runtime.universe._

object StringExt {
  val SINGLE = "'"
  val DOUBLE = "\""
  val BACK = "`"
  val ALL = Seq(SINGLE, DOUBLE, BACK)

  implicit class BetterString(s: String) {
    def replaceByMap(replacements: Map[String, String]): String = replacements.foldLeft(s) { case (a, b) => a.replaceAllLiterally(b._1, b._2) }
    def replaceBackslash = s.replace("\\", "\\\\")

    def removeExtraSpaces = s.replaceAll("\\s+", " ").trim
    def addSurroundedChars(c: String) = s"$c$s$c"
    def addExtraSpaces = addSurroundedChars(" ")
    def addSurroundedBackticks = addSurroundedChars("`")
    
    def toDoubleSafe = {
      Try(s.toDouble) match {
        case Success(d) => Option(d)
        case Failure(d) => None
      }
    }
    def toIntSafe = toDoubleSafe.map(_.toInt)

    /** Replaces period (".") between variables and literals with specified char.
     */
    def withPeriodReplace(): String = withPeriodReplace("+")
    def withPeriodReplace(char: String): String = {
      def simpleReplace(_s: String) = {
          """(\D\.)|(\.\D)""".r.findAllIn(_s).toList.foldLeft(_s) {
              case (acc, i) => {
                val newv = i.replace(".", char)
                acc.replaceAllLiterally(i, newv)
              }
          }
      }
      s.withKeepQuotedText[String](simpleReplace)
    }   

    /**
     * Finds all expressions between specified quotation marks in string.
     *
     * @param quotes [[String*]] - seq of quotation marks
     * @return [[Map[String, String]]] of replacements (expression -> MD5(expression))
     */
    def getQuotesReplaceMap(quotes: String*): Map[String, String] = {
      val qStr = if (quotes.size > 0) quotes.mkString("") else ALL.mkString("")
      val quotedTextRegex = s"""([$qStr])""" + """(?:(?=(\\?))\2.)*?\1"""
      quotedTextRegex.r.findAllIn(s).toList.map { x => (x -> OtHash.md5(x)) }.toMap
    }

    /**
      * Finds all expressions between specified brackets in string.
      *
      * @param open [[String]] - open character sequence
      * @param close [[String]] - close character sequence
      * @return [[Map[String, String]]] of replacements (expression -> MD5(expression))
      */
    def getBracketsReplaceMap(open: String, close: String): Map[String, String] = {
      val quotedTextRegex = s"""$open([^$close]+)$close"""
      quotedTextRegex.r.findAllIn(s).toList.map { x => (x -> OtHash.md5(x)) }.toMap
    }

    /**
     * Executes specified function keeping expressions within quotation marks constant.
     * Use this extension if you need, for example, to split string by any char, but don't split
     * if this char is somewhere within quotation marks.
     *
     * Use vals SINGLE, DOUBLE, BACK and ALL from this object.
     *
     * Example: str.withKeepQuotes((s: String) => s.split("|"), OtSingleExt.SINGLE, OtSingleExt.DOUBLE)
     *
     * @param func [[String => A]] - function to execute over String
     * @param quotes [[String*]] - seq of quotation marks to keep expression within them
     * @return transformed string [[A]]
     *
     */
    def withKeepQuotedText[A: TypeTag](func: String => A, quotes: String*): A = {
      val replMap = s.getQuotesReplaceMap(quotes: _*)
      val replBackMap = replMap.map(_.swap)
      val strWithReplaceQuotes = s.replaceByMap(replMap)

      val res = func(strWithReplaceQuotes) match {
        case t if typeOf[A] <:< typeOf[Traversable[StatsFunc]] => t.asInstanceOf[List[StatsFunc]].map(x =>
            StatsFunc(x.newfield.replaceByMap(replBackMap),
                      x.func.replaceByMap(replBackMap), 
                      x.field.replaceByMap(replBackMap))
        )
        case t if typeOf[A] <:< typeOf[Traversable[String]] => t.asInstanceOf[List[String]].map(_.replaceByMap(replBackMap))
        case s if typeOf[A] =:= typeOf[String]       => s.asInstanceOf[String].replaceByMap(replBackMap)
        case r                                          => r
      }
      res.asInstanceOf[A]
    }    

    /**
      * Executes specified function keeping expressions within brackets constant.
      * Similar to func withKeepQuotedText but keep text between brackets
      * @param func [[String => A]] - function to execute over String
      * @param open [[String]] - open character sequence
      * @param close [[String]] - close character sequence
      * @return transformed string [[A]]
      *
      */
    def withKeepTextInBrackets[A: TypeTag](func: String => A, open: String, close: String): A = {
      val replMap = s.getBracketsReplaceMap(open, close)
      val replBackMap = replMap.map(_.swap)
      val strWithReplaceQuotes = s.replaceByMap(replMap)

      val res = func(strWithReplaceQuotes) match {
        case t if typeOf[A] =:= typeOf[Seq[String]] => t.asInstanceOf[Seq[String]].map(_.replaceByMap(replBackMap))
        case s: String              => s.replaceByMap(replBackMap)
        case r                      => r
      }
      res.asInstanceOf[A]
    }

    // Strip string with both prefix and suffix
    def strip(seq: String): String = s.stripPrefix(seq).stripSuffix(seq)
    def strip(): String = strip(" ")
    def stripBackticks(): String = strip("`")
    def replaceSingleQuotToBacktick = s.withKeepQuotedText[String]((s: String) => s.replaceAllLiterally("'", "`"), DOUBLE)

    /**
     * Escapes chars with "\\" specified in escapedCharList
     * 
     * @param escapedCharList [[String]] - string representing chars to escape
     * @return escaped string [[String]]
     */
    def escapeChars(escapedCharList: String): String = {
      escapedCharList.filter(s.contains(_)).foldLeft(s) {
          case(acc, char) => acc.replaceAllLiterally(s"$char", s"\\$char")
      }
    }

    def roundUnresolvedTokensInQuotes(): String = {
      """\$[^\$]*\$?""".r.findAllIn(s).toList.distinct.foldLeft(s) {
        (acc, item) => acc.replaceAllLiterally(item, s"'$item'")
      }
    }
  }
}
