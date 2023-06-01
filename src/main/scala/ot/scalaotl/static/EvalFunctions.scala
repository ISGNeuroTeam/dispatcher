package ot.scalaotl
package static

import org.apache.spark.sql.functions.udf
import ot.scalaotl.extensions.StringExt._

import scala.util.{Success, Try}

object EvalFunctions extends OTLSparkSession {
  val commandsOTPToSpark: Map[String, String] = Map(
    "strftime" -> "from_unixtime",
    //    "strptime" -> "unix_timestamp",
    "mvsort" -> "array_sort",
    "mvjoin" -> "array_join",
    "mvdedup" -> "array_distinct",
    "mvfilter" -> "filter",
    //"mvrange" -> "sequence",
    // "max" -> "array_max",
    // "min" -> "array_min",
    "mvappend" -> "array",
    "printf" -> "format_string")

  val dateFormatOTPToJava: Map[String, String] = Map(
    "%H" -> "HH", "%I" -> "hh", "%M" -> "mm",
    "%S" -> "ss", "%N" -> "SSS", "%3N" -> "SSS", "%z" -> "Z",
    "%d" -> "dd", "%m" -> "MM", "%b" -> "MMM",
    "%B" -> "MMMM", "%Y" -> "yyyy", "%y" -> "yy",
    "%a" -> "E", // Sun Mon Tue
    "%A" -> "EEEE", // Sunday Monday
    "%j" -> "D", // day of year
    "%Z" -> "z", // timezone abbreviate
    "%+" -> "E MMM dd HH:mm:ss z yyyy",
    "T" -> "'T'",
    "%c" -> "E MMM dd HH:mm:ss yyyy")

  val varargsFunctions: Map[String, Int] = Map(
    "mvappend" -> 0,
    "mvrange" -> 1,
    "mvindex" -> 1,
    "mvzip" -> 2,
    "tostring" -> 0,
    "array_min" -> 0,
    "array_max" -> 0)

  def mvfilterReplace(s: String): String = {
    val rexMvfilter = """filter\(match\((.*?),\s*(.*)\)\)""".r
    rexMvfilter.findAllIn(s).matchData.map(x => (x.matched, s"filter(${x.group(1)}, x -> match(x, ${x.group(2)}))")).toList.foldLeft(s) {
      case (accum, item) => accum.replaceAllLiterally(item._1, item._2)
    }
  }

  def argsReplace(_args: String): String = {
    val mapRepl = """( +|\n)""".r.replaceAllIn(_args.trim, " ").replace(" =", "=").replace("= ", "=")
      .replaceBackslash
      .withKeepQuotedText(_.roundUnresolvedTokensInQuotes())
      .replaceSingleQuotToBacktick
      .replaceByMap(commandsOTPToSpark)
    // println(mapRepl)
    mvfilterReplace(mapRepl)
  }

  def mvindex = udf {
    (column: Any, index: Seq[Any]) =>
      val col = column match {
        case c : Seq[_] => c
        case _ => null
      }
      val idx = index.map {
        case i : Int => i
        case i : Long => i.toInt
        case i : Float  => i.toInt
        case i : Double  => i.toInt
        case _ => null
      }.asInstanceOf[Seq[Int]]

      if (col==null) null
      else if(idx.exists(_ >= col.length)) null
      else
        idx.toList match {
          case first :: second :: tail => col.slice(first, second).map(_.toString)
          case first :: tail           => if (col(first)== null) null else Seq(col(first).toString)
          case _                       => col.map(_.toString)
        }
  }
  def mvrange = udf {
    (first: Any, other: Seq[Any]) =>
      val vals = (first +: other.toList).map{
        case a : Double => a
        case a : Float => a.toDouble
        case a : Long => a.toDouble
        case a : Int => a.toDouble
        case _ => null
      }.asInstanceOf[List[Double]]
      vals match {
        case List(a,b,c)=> (a to b by c).toSeq
        case List(a,b)=>(a to b by 1).toSeq
        case _ => null
      }
  }

  def mvzip = udf {
    (column1: Any, column2: Any, delim: Seq[String]) =>
    {
      val (col1,col2)  = (column1, column2) match {
        case (c1 : Seq[_], c2 : Seq[_]) => (c1,c2)
        case _ => (null, null)
      }
      val sep = delim.toList match {
        case d :: tail => d
        case _         => " "
      }
      if (col1 == null) Seq()
      else col1.zip(col2).map { case (i, j) => s"$i$sep$j"}
    }
  }

  def mvfind = udf { (column: Any, rex: String) => {
    val col = column match {
      case c : Seq[_] => c
      case _ => null
    }
    if (col == null) null
    else if (rex == null) Int.box(-1)
    else Int.box(col.zipWithIndex
      .map( t => (t._1.toString -> t._2))
      .find(t => rex.r.pattern.matcher( t._1).matches())
      .map(_._2).getOrElse(-1))
  }
  }

  def mvcount = udf {column: Any => {
    val col = column match {
      case c : Seq[_] => c
      case _ => null
    }
    if (col == null) null else Int.box(col.length)
  }}
  def len = udf { (col: String) => col.length }
  def now = udf { OtDatetime.getCurrentTimeInSeconds }
  def matched = udf {
    (col: String, rex: String) =>
    {
      rex.r.findFirstIn(col) match {
        case Some(str) => true
        case _         => false
      }
    }
  }
  def replace = udf { (s: String, rex: String, replacement: String) =>

    Option(s) match {
      case Some(_) =>
        rex.r.replaceAllIn(s, replacement)
      case None =>
        s
    }

  }
  def sha1 = udf { OtHash.sha1 }
  def truefunc = udf { () => true }
  def tonumber = udf { (col: Any) => Try(col.toString.toDoubleSafe) match {
    case Success(x) => x
    case _ => None
  }}
  def tostring = udf {
    (seq: Seq[Any]) =>
      seq.toList match {
        case x :: "duration" :: tail => {
          val totalSecs = x.toString.toDouble.toInt
          val hours = totalSecs / 3600;
          val minutes = (totalSecs % 3600) / 60;
          val seconds = totalSecs % 60;
          "%02d:%02d:%02d".format(hours, minutes, seconds)
        }
        case x :: tail => x.toString
        case _         => null
      }
  }

  def relativeTime = udf { (col: Long, relative: String) => OtDatetime.getRelativeTime(col, relative) }
  def castToMultival = udf{(x : Any) =>
    val res = if(x.isInstanceOf[Seq[Any]]) x.asInstanceOf[Seq[Any]] else Seq(x)
    res match {
      case res  => res.asInstanceOf[Seq[String]]
    }
  }

  // def convertTimeToSec = udf { (t: Long) => if (t / 1e10 < 1) t else t / 1000 }
  // def convertTimeToMilliSec = udf { (t: Double) => if (t / 1e10 < 1) (t * 1000).toLong else t.toLong }

  def strptime() = udf {
    timeFormat(dateFormatOTPToJava) // Const should be inside udf body for successful serialisation
  }

  val  timeFormat = (format: Map[String, String]) => (column: Any, strFormat: String) => {
    val col = column match {
      case c: Seq[_] if c.nonEmpty => c.head.toString
      case c: String => c
      case _ => null
    }
    val output = col match {
      case null => null
      case colValue =>
        import java.text.SimpleDateFormat
        val javaFormat = strFormat.replaceByMap(format)
        val inputFormat = new SimpleDateFormat(javaFormat)
        val epoch = inputFormat.parse(colValue)
        Long.box(epoch.getTime / 1000)
    }
    output
  }

  spark.udf.register("mvindex", mvindex)
  spark.udf.register("mvzip", mvzip)
  spark.udf.register("mvfind", mvfind)
  spark.udf.register("mvcount", mvcount)
  spark.udf.register("len", len)
  spark.udf.register("now", now)
  spark.udf.register("match", matched) // TODO. Replace single backslash to double backslash within 'match'
  spark.udf.register("replace", replace) // TODO. Replace single backslash to double backslash within 'replace'
  spark.udf.register("sha1", sha1)
  spark.udf.register("true", truefunc)
  spark.udf.register("tonumber", tonumber)
  spark.udf.register("tostring", tostring)
  spark.udf.register("relative_time", relativeTime)
  spark.udf.register("cast_to_multival", castToMultival)
  spark.udf.register("strptime", strptime)
  spark.udf.register("mvrange", mvrange)


}