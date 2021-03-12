package ot.scalaotl
package static

import java.util.regex.Pattern

import org.apache.spark.sql.functions.udf
import org.json4s._

import scala.util.matching.Regex.Match
import scala.util.{Failure, Success, Try}

class FieldExtractor extends Serializable {
  implicit val formats = DefaultFormats
  val quotesSub = "&QUOTES&"

  def parseJson(line: String, fields: Set[String]): Map[String, Any] = {
    Try(OtJsonParser.jp.parseSpaths(line, fields)) match {
      case Success(v)  => v
      case Failure(ex) => Map.empty
    }
  }

  def parseCuttedJson(line: String, fields: Set[String]): Map[String, String] = {
    val rex = """\[[^\[]*?\]|\{[^{]*?\}""".r
    rex.findAllIn(line).toArray.flatMap(l => parseJson(l, fields) match {
      case x: Map[_, _] => x.map(x => x._1 -> x._2.toString)
    }).toMap
  }

  def parseKV(line: String, fields: Set[String]): Map[String, String] = {
    val rex = """(("([^\s,;"]+)")|('([^\s,;']+)')|([^\s,;"]+))(=|:)("(.*?)"|'(.*?)'|([^\s,;]+))""".r
    val regexes = fields.map(_.replace("{}","\\{\\d+\\}").replace("*",".*").r)
    val res = rex.findAllMatchIn(line)
    res.toArray.map((m: Match) => {
      val field = Option(m.group(3))
        .orElse(Option(m.group(5)))
        .orElse(Option(m.group(6)))
        .get
      val resMap: Map[String, String] = if (regexes.exists(_.pattern.matcher(field).matches()))
        Map(field -> Option(m.group(9))
          .orElse(Option(m.group(10)))
          .orElse(Option(m.group(11))).get)
      else Map.empty
      resMap
    }).flatten.toMap
  }

  def parseNamedRegex(line: String, rex: String): Map[String, String] = {
    val namesIter = """\(\?<([a-zA-Z][a-zA-Z0-9]*)>""".r.findAllMatchIn(rex)
    val names = namesIter.map(i => i.group(1)).toArray
    val matcher = Pattern.compile(rex).matcher(line)
    if ({ matcher.find }) names.map(n => (n, matcher.group(n))).toMap else Map.empty
  }

  def parseRegex(line: String, regexes: Map[String, String]): Map[String, String] = {
    regexes.map(r => {
      val res = Try(r._2.r) match {
        case Success(v) => parseNamedRegex(line, r._2).get(r._1).orElse(v.findFirstIn(line))
        case Failure(ex) => //TODO Logger.error(ex)
          Option.empty
      }
      (r._1, res)
    })
      .filter(r => r._2.isDefined).map(r => (r._1, r._2.get))
  }

//  def parseAny(line: String, fields: Set[String], regexes: Map[String, String]): Map[String, String] = {
//    val modifLine = line.replace("\\\"", quotesSub)//.replace(".", dotSub)
//    var parsed = parseJson(modifLine, fields) match {case x: Map[String, Any] => x}
//    if (parsed.isEmpty) parsed = parseCuttedJson(modifLine, fields)
//    if (parsed.isEmpty) parsed = parseKV(modifLine, fields)
//    val res = parsed.map(e => (e._1, e._2 match {
//      case x:String => x.replace(quotesSub, "\\\"")
//      case x : List[String]=> x.map(_.replace(quotesSub, "\\\""))
//        .mkString("[",",","]")
//      case None => null
//      case x => x.toString
//    }))
//    res ++ parseRegex(line, regexes)
//  }

  def parseMVAny(line: String, fields: Set[String], regexes: Map[String, String]): Map[String, List[String]] = {
    val modifLine = line.replace("\\\"", quotesSub)
    var parsed = parseJson(modifLine, fields) match {
      case l: Map[String, Any] =>
        l.map(
          e => (e._1, e._2 match {
          case x: String => x.replace (quotesSub, "\\\"")
          case _ => null
          }))
    }
   // if (parsed.isEmpty) parsed = parseCuttedJson(modifLine, fields).map{case (k,v) => (k, List(v))}
    if (parsed.isEmpty) parsed = parseKV(modifLine, fields)//.map{case (k,v) => (k, List(v))}
    parsed.toList.groupBy(f => f._1.replaceAll("\\{\\d+\\}","{}"))//fields.contains(f._1) || if(fields.exists(_.contains("*"))) f._1 else
      .map{case (k,v) => (k, v.sortBy(_._1).map(_._2))}
      .filter(x => x._1.contains("{}") || x._2.length == 1)
  }
}

/**
 * Example:
 * df.withColumn("raw", F.lit(raw))
 *   .withColumn("fields", F.array(F.lit("Area"), F.lit("TagName")))
 *   .withColumn("parse", FieldExtractor.extractUDF(F.col("raw"), F.col("fields")))
 *   .show
 */
object FieldExtractor {
  @transient lazy val fe = new FieldExtractor()
  def extractUDF = udf((str: String, fields: Seq[String]) => fe.parseMVAny(str, fields.toSet, Map()))

}