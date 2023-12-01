package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, typedLit, udf}
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.static.OtHash

import scala.util.matching.Regex

class OTLRex(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("field", "max_match", "mode")
  val keywordsRex: Map[String, String] = """(^|\s)(max_match|field|mode)=(\S+)""".r.findAllIn(args).matchData.map { x => x.group(2) -> x.group(3) }.toMap
  val regexStr: String = keywordsRex.map { case (k, v) => s"$k=$v" }.foldLeft(args) { (a, b) => a.replace(b, "") }.trim.drop(1).dropRight(1)

  //Replaces allows the use of a symbol '_' in group names
  val replMap: Map[String, String] = getGroupReplaces(regexStr)
  val normalisedRegexStr: String = regexStr.replaceByMap(replMap)
  val replBackMap: Map[String, String] = replMap.map(m => m._1.substring(2, m._1.length-1) -> m._2.substring(2, m._2.length-1)).map(_.swap)

  val groupNamesIter: Iterator[Regex.Match] = """\(\?<([a-zA-Zа-яА-Я][a-zA-Zа-яА-Я0-9]*)>""".r.findAllMatchIn(normalisedRegexStr)
  val groupNames: Array[String] = groupNamesIter.map(i => i.group(1)).toArray

  object Udfs extends Serializable { //Needed to make serialisable udfs with calls from one function to another function
    val parseNamedRegex: (String, String, String, Seq[String]) => Map[String, Seq[String]] = (line: String, rex: String, maxMatch: String, groupNames: Seq[String]) => {
      if (line == null) Map[String, Seq[String]]()
      else {
        val matcher = java.util.regex.Pattern.compile(rex).matcher(line)
        var res: Map[String, List[String]] = Map()
        var i = 0
        val maxMatchUnlim = if (maxMatch.toInt == 0) Integer.MAX_VALUE else maxMatch.toInt
        while (i < maxMatchUnlim && matcher.find) {
          res = groupNames.foldLeft(res) { case (r, n) => r + (n -> (r.getOrElse(n, List()) :+ matcher.group(n))) }
          i += 1
        }
        res
      }
    }
    val parseMvNamedRegex: (Seq[String], String, String, Seq[String]) => Map[String, Seq[String]] = (lines: Seq[String], rex: String, maxMatch: String, groupNames: Seq[String]) => {
      val parseRegex = parseNamedRegex
      //val merge = mergeMaps
      if (lines == null) Map[String, Seq[String]]()
      else lines.foldLeft(Map[String, Seq[String]]())((acc, line) => mergeMaps(acc, parseRegex(line, rex, maxMatch, groupNames))
      )
    }
    val mergeMaps: (Map[String, Seq[String]], Map[String, Seq[String]]) => Map[String, Seq[String]] = (a: Map[String, Seq[String]], b: Map[String, Seq[String]]) => {
      val merged = a.toSeq ++ b.toSeq
      val grouped = merged.groupBy(_._1)
      grouped.mapValues(_.foldLeft(Seq[String]())((acc, l) => acc ++ l._2).toSeq)
    }
  }

  val extractUDF: UserDefinedFunction = udf(Udfs.parseNamedRegex)
  val extractMultivalUDF: UserDefinedFunction = udf(Udfs.parseMvNamedRegex)

  override val fieldsUsed: List[String] = keywordsRex.get("field") match {
    case Some(f) => List(f)
    case _ => List[String]()
  }

  override def transform(_df: DataFrame): DataFrame = {
    val field = keywordsRex("field")
    if (_df.getColumTypeName(field) == "null") return _df
    val df: DataFrame = _df.withColumn("rex", lit(normalisedRegexStr))
      .withColumn("group_names", typedLit(groupNames))
      .withColumn("max_match", lit(keywordsRex.getOrElse("max_match", "1")))
    //If input column is of array type take first element
    val sdf = df.withColumn("used_" + field, col(field))
    val rdf = if (_df.getColumTypeName(field) == "array")
      sdf.withColumn("dict", extractMultivalUDF(col("used_" + field), col("rex"), col("max_match"), col("group_names")))
    else {
      sdf.withColumn("dict", extractUDF(col("used_" + field), col("rex"), col("max_match"), col("group_names")))
    }
    val mrdf = rdf.drop("rex", "max_match", "group_names", "used_" + field)

    groupNames.foldLeft(mrdf) {
      case (a, b) => if (keywordsRex.getOrElse("max_match", "1") == "1")
        a.withColumn(b.replaceByMap(replBackMap), col("dict").getItem(b).getItem(0))
      else
        a.withColumn(b.replaceByMap(replBackMap), col("dict").getItem(b))

    }.drop("dict")
  }

  def getGroupReplaces(str: String): Map[String, String] = {
    val rexStr = """\?<(([A-Za-zА-Яа-я0-9_])*)>"""
    rexStr.r.findAllIn(str).matchData.map {
      x =>
        x.group(0) -> ("?<" + "x" + OtHash.md5(x.group(1)) + ">") }.toMap
  }
}
