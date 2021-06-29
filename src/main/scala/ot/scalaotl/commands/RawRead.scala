package ot.scalaotl
package commands

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, functions => F}
import org.json4s._
import org.json4s.native.JsonMethods._
import ot.scalaotl.config.OTLIndexes
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.{ExpressionParser, WildcardParser}
import ot.scalaotl.utils.searchinternals._

class RawRead(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLIndexes with ExpressionParser with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("limit", "subsearch")

  val SimpleQuery(_args, searchId, cache, subsearches, tws, twf, stfe, preview) = sq

  override def validateOptionalKeywords(): Unit = ()
  override def getFieldsFromExpression(expr: Expression, fields: List[String]): List[String] = {
    val newfields = if (expr.nodeName == "Literal") {
      expr.toString.stripPrefix("'") :: fields
    } else fields
    val children = expr.children
    if (children.nonEmpty) {
      children.foldLeft(fields) {
        (accum, item) => accum ++ getFieldsFromExpression(item, newfields)
      }
    } else newfields
  }

  def jsonStrToMap(jsonStr: String): Map[String, Map[String, String]] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Map[String, String]]]
  }

  val indexQueriesMapFirst = jsonStrToMap(excludeKeywords(_args.trim, List(Keyword("limit", "t"))))
  var indexQueriesMap = indexQueriesMapFirst
  val allIndexes = getAllIndexes()
  for (index <- indexQueriesMap) {
    if (index._1.contains("*")) {
      val regex_raw = index._1.replace("*", ".*").r
      log.debug(s"[SearchID:$searchId] filterRegex for maskIndex : $regex_raw")
      val mask_indexes = allIndexes filter (x => regex_raw.pattern.matcher(x).matches())
      log.debug(s"[SearchID:$searchId] maskIndexes : $mask_indexes")
      indexQueriesMap -= index._1
      mask_indexes.map(x => indexQueriesMap = indexQueriesMap + (x -> index._2))
    }
  }

  override val fieldsUsed = indexQueriesMap.map {
    case (_, singleIndexMap) => singleIndexMap.getOrElse("query", "").withKeepQuotedText[List[String]](
      (s: String) => """(?![!\(])(\S*?)\s*(=|>|<|like|rlike)\s*""".r.findAllIn(s).matchData.map(_.group(1)).toList
    )
      .map(_.strip("!").strip("'").strip("\"").stripBackticks.addSurroundedBackticks)
  }.toList.flatten

  def modifyItemQuery(i: (String, Map[String, String])) = {
    val query = i._2.getOrElse("query", "")
    val backtickedQuery = fieldsUsedInFullQuery.foldLeft(query)((q, field) => {
      val nf = field.replace("{", "[").replace("}", "]").addSurroundedBackticks
      q.replaceAll("""(\(| )(['`]*\Q""" + field + """\E['`]*)\s*(=|>|<|!=| like| rlike)""", s"$$1$nf$$3")
        .replace(nf + "=\"null\"", s"$nf is null")
        .replace(nf + "!=\"null\"", s"$nf is not null")
    })
    (i._1, i._2 + ("query" -> backtickedQuery))
  }

  def getModifedQuery(i: (String, Map[String, String])) = {
    val query = i._2.getOrElse("query", "")
    fieldsUsedInFullQuery.foldLeft(query)((q, field) => {
      val nf = field.addSurroundedBackticks//.replace("{", "[").replace("}", "]")
      q.replaceAll("""(\(| )(['`]*\Q""" + field + """\E['`]*)\s*(=|>|<|!=| like| rlike)""", s"$$1$nf$$3")
        .replace(nf + "=\"null\"", s"$nf is null")
        .replace(nf + "!=\"null\"", s"$nf is not null")
    })
  }

  private def searchMap(query: Map[String, Map[String, String]]): DataFrame = {
    /**
      * Replace all {} symbols to [] in fieldnames in query.
      * Then replaces all quotes around fieldnames to bacticktics.
      * Then replaces fieldname=\"null\" substrings to 'fieldname is null'
      * Fieldnames are taken from `fieldsUsedInFullQuery`
      *
      * @param i [[ String, Map[String, String] ]] - item with original query
      * @return [[ String, Map[String, String] ]] - modified item
      */


    val (df, allExceptions) = query.foldLeft((spark.emptyDataFrame.asInstanceOf[DataFrame], List[Exception]())) {
      case (accum, item) => {
       // val mItem = modifyItemQuery(item)
        log.debug(s"[SearchID:$searchId]Query is " + item)
        //log.debug(s"[SearchID:$searchId]Modified query is" + mItem)
        val modifiedQuery = getModifedQuery(item)
        val nItem = (item._1, item._2 + ("query" -> ""))

        val s = new IndexSearch(spark, log, nItem, searchId, Seq[String](), preview)
        try {
          val fdfe: DataFrame = extractFields(s.search())
          val ifdfe = if(fieldsUsedInFullQuery.contains("index"))
            fdfe.drop("index").withColumn("index", lit(item._1))
          else
            fdfe
          val fdf = if(modifiedQuery == "") ifdfe else ifdfe.filter(modifiedQuery)
          val cols1 = fdf.columns.map(_.stripBackticks().addSurroundedBackticks).toSet
          val cols2 = accum._1.columns.map(_.stripBackticks().addSurroundedBackticks).toSet
          val totalCols = (cols1 ++ cols2).toList

          def expr(myCols: Set[String], allCols: Set[String]) = {
            allCols.toList.map(x => if (myCols.contains(x)) F.col(x).as(x.stripBackticks) else F.lit(null).as(x.stripBackticks))
          }

          totalCols match {
            case head :: tail => (fdf.select(expr(cols1, totalCols.toSet): _*).union(accum._1.select(expr(cols2, totalCols.toSet): _*)), accum._2)
            case _ => accum
          }
        } catch {
          case ex: Exception => (accum._1, ex +: accum._2)
        }
      }
    }
    if (query.size == allExceptions.size) throw allExceptions.head

    // Add columns which are used in query but does not exist in dataframe after read
    val emptyCols = fieldsUsedInFullQuery
      .map(_.stripBackticks())
      .distinct
      .filterNot(_.contains("*"))
      .diff(df.columns.toSeq)
    log.debug(s"""[SearchID:$searchId] Add null cols to dataframe: [${emptyCols.mkString(", ")}]""")
    emptyCols.foldLeft(df) {
      (acc, col) => acc.withColumn(col, F.lit(null))
    }
  }

  private def extractFields(df: DataFrame): DataFrame = {
    import ot.scalaotl.static.FieldExtractor

    val stfeFields = fieldsUsedInFullQuery.diff(df.notNullColumns)
    log.debug(s"[SearchID:$searchId] Search-time field extraction: $stfeFields")
    val valueFields = stfeFields.filter(!_.contains("{}"))
    val multiValueFields = stfeFields.filter(_.contains("{}"))
    val feDf = makeFieldExtraction(df,stfeFields, FieldExtractor.extractUDF)
    // makeFieldExtraction(feDf, multiValueFields, FieldExtractor.extractMVUDF)
    feDf
  }

  private def makeFieldExtraction(df: DataFrame,
                                  extractedFields: Seq[String],
                                  udf: UserDefinedFunction
                                 ): DataFrame = {
    import org.apache.spark.sql.functions.{col, expr}
    val stfeFieldsStr = extractedFields.map(x => s""""${x.replaceAll("\\{(\\d+)\\}", "{}")}"""").mkString(", ")
    val mdf = df.withColumn("__fields__", expr(s"""array($stfeFieldsStr)"""))
      .withColumn("stfe", udf(col("_raw"), col("__fields__")))
    val fields : Seq[String] = if(extractedFields.exists(_.contains("*"))){
      val sdf = mdf.agg(flatten(collect_set(map_keys(col("stfe")))).as("__schema__"))
      sdf.first.getAs[Seq[String]](0)
    } else extractedFields
    val existedFields = mdf.notNullColumns
    fields.foldLeft(mdf) { (acc, f) => {
      if (!existedFields.contains(f)) {
        if (f.contains("{}"))
          acc.withColumn(f, col("stfe")(f))
        else {
          val m = "\\{(\\d+)\\}".r.pattern.matcher(f)
          var index = if (m.find()) m.group(1).toInt - 1 else 0
          index = if (index < 0) 0 else index
          acc.withColumn(f, col("stfe")(f.replaceFirst("\\{\\d+\\}", "{}"))(index))
        }
      } else acc
    }
    }.drop("__fields__", "stfe")
  }


  override def transform(_df: DataFrame): DataFrame = {

    log.debug(s"searchId = $searchId queryMap: $indexQueriesMap")
    val dfInit = searchMap(indexQueriesMap)
    val dfLimit = getKeyword("limit") match {
      case Some(lim) => {
        log.debug(s"[SearchID:$searchId] Dataframe is limited"); dfInit.limit(100000)
      }
      case _ => dfInit
    }

    val dfStfe = dfLimit
    getKeyword("subsearch") match {
      case Some(str) => {
        cache.get(str) match {
          case Some(jdf) => new OTLJoin(SimpleQuery(s"""type=inner max=1 ${jdf.columns.toList.mkString(",")} subsearch=$str""", cache)).transform(dfStfe)
          case None => dfStfe
        }
      }
      case None => dfStfe
    }
  }
}
