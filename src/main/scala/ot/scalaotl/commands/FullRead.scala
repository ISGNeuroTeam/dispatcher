package ot.scalaotl
package commands

import ot.scalaotl.utils.searchinternals._
import ot.scalaotl.config.OTLIndexes
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.parsers.{ExpressionParser, WildcardParser}
import ot.scalaotl.static.EvalFunctions
import org.apache.spark.sql.{DataFrame, functions => F}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.commons.lang.StringEscapeUtils.escapeJava
import org.apache.spark.sql.expressions.UserDefinedFunction

class FullRead(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLIndexes with ExpressionParser with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("limit", "subsearch")
  val SimpleQuery(_args, searchId, cache, subsearches, tws, twf, stfe, preview) = sq

  override def validateOptionalKeywords(): Unit = ()

  def jsonStrToMap(jsonStr: String): Map[String, Map[String, String]] = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Map[String, String]]]
  }

  val indexQueriesMapFirst: Map[String, Map[String, String]] = jsonStrToMap(excludeKeywords(_args.trim, List(Keyword("limit", "t"))))
  var indexQueriesMap: Map[String, Map[String, String]] = indexQueriesMapFirst
  val allIndexes = getAllIndexes()
  for (index <- indexQueriesMap) {
    if (index._1.contains("*")) {
      val regex_raw = index._1.replace("*", ".*").r
      log.debug(s"[SearchID:$searchId] filterRegex for maskIndex : $regex_raw")
      val mask_indexes = allIndexes filter (x => regex_raw.pattern.matcher(x).matches())
      log.debug(s"[SearchID:$searchId] maskIndexes : $mask_indexes")
      indexQueriesMap -= index._1
      mask_indexes.foreach(x => indexQueriesMap = indexQueriesMap + (x -> index._2))
    }
  }

  override val fieldsUsed: List[String] = indexQueriesMap.map {
    case (_, singleIndexMap) => singleIndexMap.getOrElse("query", "").withKeepQuotedText[List[String]](
      (s: String) => """(?![!\(])(\S*?)\s*(=|>|<|like|rlike)\s*""".r.findAllIn(s).matchData.map(_.group(1)).toList
    )
      .map(_.strip("!").strip("'").strip("\"").stripBackticks().addSurroundedBackticks)
  }.toList.flatten

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
    def modifyItemQuery(i: (String, Map[String, String])) = {
      val query = i._2.getOrElse("query", "")
      val backtickedQuery = fieldsUsedInFullQuery
        .filter(!_.matches("""^[0-9]*$"""))//Filtering not number names
        .foldLeft(query)((q, field) => {
        val nf = field.replace("{", "[").replace("}", "]").addSurroundedBackticks
        q.replaceAll("""(\(| )(['`]*\Q""" + field + """\E['`]*)\s*(=|>|<|!=| like| rlike)""", s"$$1$nf$$3")
          .replace(nf + "=\"null\"", s"$nf is null")
          .replace(nf + "!=\"null\"", s"$nf is not null")
      })
      (i._1, i._2 + ("query" -> backtickedQuery))
    }

    val (df, allExceptions) = query.foldLeft((spark.emptyDataFrame, List[Exception]())) {
      case (accum, item) =>
        val mItem = modifyItemQuery(item)
        log.debug(s"[SearchID:$searchId]Query is " + item)
        log.debug(s"[SearchID:$searchId]Modified query is" + mItem)
        fieldsUsedInFullQuery = item._2.get("query") match{
          case Some(x) if x != "" => getFieldsFromExpression(F.expr(x).expr,List()) ++ fieldsUsedInFullQuery
          case Some(_) | None => fieldsUsedInFullQuery
        }
        val s = new IndexSearch(spark, log, mItem, searchId, fieldsUsedInFullQuery, preview, true)
        try {
          val fdf: DataFrame = s.search()
          log.debug(s"[SearchID:$searchId] fdf.schema: ${fdf.schema} ")
          val cols1 = fdf.columns.map(_.stripBackticks().addSurroundedBackticks).toSet
          val cols2 = accum._1.columns.map(_.stripBackticks().addSurroundedBackticks).toSet
          val totalCols = (cols1 ++ cols2).toList

          def expr(myCols: Set[String], allCols: Set[String]) = {
            allCols.toList.map(x => if (myCols.contains(x)) F.col(x).as(x.stripBackticks()) else F.lit(null).as(x.stripBackticks()))
          }

          totalCols match {
            case head :: tail => (fdf.select(expr(cols1, totalCols.toSet): _*).union(accum._1.select(expr(cols2, totalCols.toSet): _*)), accum._2)
            case _ => accum
          }
        } catch {
          case ex: Exception => (accum._1, ex +: accum._2)
        }
    }
    if (query.size == allExceptions.size) throw allExceptions.head

    // Add new columns with arrays of fields by mask ..[\d]...
    // Replace square brackets to curly brackets in column names
    val bracketCols = df.columns.filter("""^.*\[\d+\].*$""".r.pattern.matcher(_).matches)
    val dfWithArrays = bracketCols
      .groupBy(_.replaceAll("\\[\\d+\\]", "{}"))
      .filterKeys(key => {
        fieldsUsedInFullQuery.map(_.stripBackticks().escapeChars("""<([{\^-=$!|]})?+.>""").replace("*", ".*").r)
          .exists(_.pattern.matcher(key).matches())
      })
      .mapValues(x => x.map(_.addSurroundedBackticks).sorted.mkString(", "))
      .foldLeft(df) {
        case (acc, (newfield, arrayStr)) =>
          acc.withColumn(newfield, F.expr(s"filter(array($arrayStr), x -> x is not null)")) //
      }
    val addedCols = dfWithArrays.columns.diff(df.columns).mkString(", ")
    log.debug(s"""[SearchID:$searchId] Add multi-valued columns: [$addedCols]""")

    val dfColsRenamed = bracketCols.foldLeft(dfWithArrays) {
      (acc, col) =>
        if(fieldsUsedInFullQuery.contains(col.replaceByMap(Map("[" -> "{", "]" -> "}"))))
        acc.withSafeColumnRenamed(
          col, col.replaceByMap(Map("[" -> "{", "]" -> "}"))
        ) else acc.drop(col)
    }
    log.debug(s"""[SearchID:$searchId] DF cols after adding mv-columns: [${dfColsRenamed.columns.mkString(", ")}]""")
    dfColsRenamed
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
    import org.apache.spark.sql.functions.{ col, expr }
    val stfeFieldsStr = extractedFields.map(x => s""""$x"""").mkString(", ")
    extractedFields.foldLeft(
      df.withColumn("__fields__", expr(s"""array($stfeFieldsStr)"""))
        .withColumn("stfe", udf(col("_raw"), col("__fields__")))
    ){
      (acc, f) => if(f.contains("{}"))
        acc.withColumn(f, col("stfe")(f))
      else acc.withColumn(f, col("stfe")(f)(0))
    }.drop("__fields__", "stfe")
  }


  override def transform(_df: DataFrame): DataFrame = {

    log.debug(s"[SearchId:$searchId] queryMap: $indexQueriesMap")
    val dfInit = searchMap(indexQueriesMap)
    log.debug(s"[SearchId:$searchId] dfInit.schema: ${dfInit.schema}")
    val dfLimit = getKeyword("limit") match {
      case Some(lim) =>
        log.debug(s"[SearchID:$searchId] Dataframe is limited")
        dfInit.limit(100000)
      case _ => dfInit
    }

    val dfStfe = dfLimit
    getKeyword("subsearch") match {
      case Some(str) =>
        cache.get(str) match {
          case Some(jdf) => new OTLJoin(SimpleQuery(s"""type=inner max=1 ${jdf.columns.toList.mkString(",")} subsearch=$str""", cache)).transform(dfStfe)
          case None => dfStfe
        }
      case None => dfStfe
    }
  }
}
