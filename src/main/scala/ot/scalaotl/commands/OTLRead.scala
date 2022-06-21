package ot.scalaotl
package commands

import ot.scalaotl.utils.searchinternals._
import ot.scalaotl.config.OTLIndexes
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.parsers.{ExpressionParser, WildcardParser}
import org.apache.spark.sql.{DataFrame, functions => F}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.collection.mutable.ListBuffer

/** =Abstract=
 * This class provides support of __'''search'''__ otl command.
 *
 * __'''search'''__ used for read raw data from indexes (only fields _raw and time)
 * field _raw is used for search-time-field-extraction
 * The command can search for given words inside the _raw field
 * Search words can be combined via logical operations AND and OR
 *
 * =Usage example=
 * OTL: one index and one search word
 * {{{ search index="index_name" "search_word" | ... other otl-commands }}}
 * OTL: two indexes and one search word
 * {{{ search index="first_index_name" index="second_index_name" "first_search_word" | ... other otl-commands }}}
 * OTL: one index and two search words
 * {{{ search index="index_name" "first_search_word" AND "second_search_word" | ... other otl-commands }}}
 *
 *  Also search command can be used in subqueries
 *
 * @constructor creates new instance of [[OTLRead]]
 * @param sq [[SimpleQuery]]
 */
class OTLRead(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLIndexes with ExpressionParser with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords: Set[String] = Set("limit", "subsearch")

  val SimpleQuery(_args, searchId, cache, subsearches, tws, twf, stfe, preview) = sq

  def jsonStrToMap(jsonStr: String): Map[String, Map[String, String]] = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Map[String, String]]]
  }

  // Get a list of available indexes (allIndexes)
  val allIndexes: ListBuffer[String] = getAllIndexes()
  // Search in the query for all indexes containing * and look for similar ones among the available indexes
  var indexQueriesMap: Map[String, Map[String, String]] = jsonStrToMap(excludeKeywords(_args.trim, List(Keyword("limit", "t"))))
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

  // Command OTLRead has no optional keywords, nothing to validate
  override def validateOptionalKeywords(): Unit = ()

  /**
   * Reads data from indexes
   * Determines which fields can be obtained from indexes and which cannot
   * Then adds not found fields as null columns
   *
   * @param query [[Map[String, Map[String, String]]] - source query
   * @return df [[DataFrame]] - dataframe with index time fields
   */
  private def searchMap(query: Map[String, Map[String, String]]): DataFrame = {

    /**
     * Replaces all {} symbols to [] in fieldnames in query.
     * Then replaces all quotes around fieldnames to backticks.
     * Then replaces fieldname=\"null\" substrings to 'fieldname is null'
     * Fieldnames are taken from `fieldsUsedInFullQuery`
     *
     * @param i [[ String, Map[String, String] ]] - item with original query
     * @return [[ String, Map[String, String] ]] - modified item
     */
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

    val (df, allExceptions) = query.foldLeft((spark.emptyDataFrame, List[Exception]())) {
      case (accum, item) =>
        val mItem = modifyItemQuery(item)
        log.debug(s"[SearchID:$searchId]Query is " + item)
        log.debug(s"[SearchID:$searchId]Modified query is" + mItem)
        val s = new IndexSearch(spark, log, mItem, searchId, fieldsUsedInFullQuery, preview)
        try {
          val fdf: DataFrame = s.search()
          val cols1 = fdf.columns.map(_.stripBackticks().addSurroundedBackticks).toSet
          val cols2 = accum._1.columns.map(_.stripBackticks().addSurroundedBackticks).toSet
          val totalCols = cols1 ++ cols2
          def expr(myCols: Set[String], allCols: Set[String]) = {
            allCols.toList.map(x => if (myCols.contains(x)) F.col(x).as(x.stripBackticks()) else F.lit(null).as(x.stripBackticks()))
          }

          if (totalCols.nonEmpty) (fdf.select(expr(cols1, totalCols): _*).union(accum._1.select(expr(cols2, totalCols): _*)), accum._2)
          else accum
        } catch {
          case ex: Exception => (accum._1, ex +: accum._2)
        }
    }
    if (query.size == allExceptions.size) throw allExceptions.head

    // Add new columns with arrays of fields by mask ..[\d]...
    // Replace [ ] brackets to { } brackets in column names
    val bracketCols = df.columns.filter("""^.*\[\d+\].*$""".r.pattern.matcher(_).matches)
    val dfWithArrays = bracketCols
      .groupBy(_.replaceAll("\\[\\d+\\]", "{}"))
      .filterKeys(fieldsUsedInFullQuery.map(_.stripBackticks()).contains)
      .mapValues(x => x.map(_.addSurroundedBackticks).sorted.mkString(", "))
      .foldLeft(df) {
        case (acc, (newfield, arrayStr)) =>
          acc.withColumn(newfield, F.expr(s"filter(array($arrayStr), x -> x is not null)")) //
      }
    val addedCols = dfWithArrays.columns.diff(df.columns).mkString(", ")
    log.debug(s"""[SearchID:$searchId] Add multi-valued columns: [$addedCols]""")

    val dfColsRenamed = bracketCols.foldLeft(dfWithArrays) {
      (acc, col) =>
        acc.withSafeColumnRenamed(
          col, col.replaceByMap(Map("[" -> "{", "]" -> "}"))
        )
    }
    log.debug(s"""[SearchID:$searchId] DF cols after adding mv-columns: [${dfColsRenamed.columns.mkString(", ")}]""")

    // Add columns which are used in query but does not exist in dataframe as null values
    val emptyCols = fieldsUsedInFullQuery
      .map(_.stripBackticks())
      .distinct
      .filterNot(_.contains("*"))
      .diff(dfColsRenamed.columns.toSeq)
    log.debug(s"""[SearchID:$searchId] Add null cols to dataframe: [${emptyCols.mkString(", ")}]""")
    emptyCols.foldLeft(dfColsRenamed) {
      (acc, col) => acc.withColumn(col, F.lit(null))
    }
  }



  /**
   * Finds fields from the query that are not among the non-empty fields of the dataframe
   * then makes field extraction for these search-time-field-extraction Fields (by calling makeFieldExtraction)
   * and adds them to dataframe
   *
   * @param df [[DataFrame]] - source dataframe
   * @return df [[DataFrame]] - dataframe with search time fields
   */
  private def extractFields(df: DataFrame): DataFrame = {
    import ot.scalaotl.static.FieldExtractor

    val stfeFields = fieldsUsedInFullQuery.diff(df.notNullColumns)
    log.debug(s"[SearchID:$searchId] Search-time field extraction: $stfeFields")
    val feDf = makeFieldExtraction(df, stfeFields, FieldExtractor.extractUDF)
    feDf
  }


  /**
   * Makes field extraction for these search-time-field-extraction Fields
   * And adds them to dataframe
   *
   * @param df [[DataFrame]] - source dataframe
   * @param extractedFields [[ Seq[String] ]] - list of fields for search-time-field-extraction
   * @param udf [[ UserDefinedFunction ]] - UDF-function for fields extraction
   * @return df [[DataFrame]] - dataframe with search time fields
   */
  private def makeFieldExtraction(df: DataFrame, extractedFields: Seq[String], udf: UserDefinedFunction): DataFrame = {
    import org.apache.spark.sql.functions.{col, expr}

    val stfeFieldsStr = extractedFields.map(x => s""""$x"""").mkString(", ")
    extractedFields.foldLeft(
      df.withColumn("__fields__", expr(s"""array($stfeFieldsStr)"""))
        .withColumn("stfe", udf(col("_raw"), col("__fields__")))
    ) {
      (acc, f) =>
        if (f.contains("{}"))
          acc.withColumn(f, col("stfe")(f))
        else acc.withColumn(f, col("stfe")(f)(0))
    }.drop("__fields__", "stfe")
  }


  override def transform(_df: DataFrame): DataFrame = {

    log.debug(s"searchId = $searchId queryMap: $indexQueriesMap")
    val dfInit = searchMap(indexQueriesMap)
    val dfLimit = getKeyword("limit") match {
      case Some(lim) => log.debug(s"[SearchID:$searchId] Dataframe is limited to $lim"); dfInit.limit(100000)
      case _ => dfInit
    }
    val dfStfe = if (stfe) extractFields(dfLimit) else dfLimit
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
