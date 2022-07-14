package ot.scalaotl
package commands

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, functions => F}
import org.json4s._
import org.json4s.native.JsonMethods._
import ot.scalaotl.config.OTLIndexes
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.parsers.{ExpressionParser, WildcardParser}
import ot.scalaotl.utils.searchinternals._

import scala.collection.mutable.ListBuffer

/** =Abstract=
 * This class provides support of __'''search'''__ otl command.
 *
 * __'''search'''__ used for read raw data from indexes (only _raw and _time fields)
 * Field _raw is used for search-time-field-extraction
 * The command can search for given words inside the _raw field
 * Search words can be combined via logical operations AND and OR
 * Logical operations names (AND and OR) are not case sensitive
 * The command can read data from multiple indexes (in this case indexes will be combined with logical OR)
 * Index names and search words can use wildcards
 *
 * =Usage example=
 * OTL: one index and one search word
 * {{{ search index="index_name" "search_word" | ... other otl-commands }}}
 *
 * OTL: two indexes and one search word
 * {{{ search index="first_index_name" index="second_index_name" "first_search_word" | ... other otl-commands }}}
 *
 * OTL: one index and two search words
 * {{{ search index="index_name" "first_search_word" AND "second_search_word" | ... other otl-commands }}}
 *
 * Also search command can be used in subqueries to filter the results of other queries
 * {{{ other otl-commands ... | search metric=temp* AND level=WARN* }}}
 *
 * @constructor creates new instance of [[ RawRead ]]
 * @param sq [[ SimpleQuery ]] - contains args, cache, subsearches, search time interval, stfe and preview flags
 */
class RawRead(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLIndexes with ExpressionParser with WildcardParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords: Set[String] = Set("limit", "subsearch")
  val SimpleQuery(_args, searchId, cache, subsearches, tws, twf, stfe, preview) = sq
  // Command has no optional keywords, nothing to validate
  override def validateOptionalKeywords(): Unit = ()

  def jsonStrToMap(jsonStr: String): Map[String, Map[String, String]] = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Map[String, String]]]
  }

  // Get a list of available indexes (reading from filesystem)
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

  // Get a list of fields used in query (this list next used for build fieldsUsedInFullQuery)
  // Select query from each entry in indexQueriesMap
  // For each entry find in query substrings which not starting with ! or ( (may be counted values and inversion)
  // and containing substrings of the form: field name =|>|<|like|rlike value
  // For all found field names do strip ! and ' and \"
  // Then strip backtics and add surrounded backticks
  override val fieldsUsed: List[String] = indexQueriesMap.map {
    case (_, singleIndexMap) => singleIndexMap.getOrElse("query", "").withKeepQuotedText[List[String]](
      (s: String) => """(?![!(])(\S*?)\s*(=|>|<|like|rlike)\s*""".r.findAllIn(s).matchData.map(_.group(1)).toList
    )
      .map(_.strip("!").strip("'").strip("\"").stripBackticks().addSurroundedBackticks)
  }.toList.flatten

  /** Modifies the query item in several steps
   * For all fieldnames in the fieldsUsedInFullQuery list following steps are performed
   * Step 1. All occurrences of the fieldname in substrings of the form "(fieldname" and " fieldname"
   * followed by characters =, >, <, !=, like, rlike are replaced to fieldname surrounded with backticks
   * Step 2. All occurrences of the fieldname in substrings of the form fieldname="null" and fieldname!="null"
   * are replaced to 'fieldname is null' and 'fieldname is not null' respectively
   * Earlier in this method was the replacement of {} brackets to []
   *
   * @param i [[ String, Map[String, String] ]] - item with original query
   * @return [[ String ]] - modified item
   */
  def getModifedQuery(i: (String, Map[String, String])): String = {
    val query = i._2.getOrElse("query", "")
    fieldsUsedInFullQuery.foldLeft(query)((q, field) => {
      val nf = field.addSurroundedBackticks
      // Here a regular expression is used instead of a simple replacement,
      // because the name of one field can be a substring of another field
      // Starts with a bracket or space, then a quotation mark or backtick
      q.replaceAll("""([\(| ])(['`]*\Q""" + field + """\E['`]*)\s*(=|>|<|!=| like| rlike)""", s"$$1$nf$$3")
        .replace(nf + "=\"null\"", s"$nf is null")
        .replace(nf + "!=\"null\"", s"$nf is not null")
    })
  }

  /**
   * Reads data from indexes
   * Determines which fields can be obtained from indexes and which cannot
   * Columns not found are added as null columns
   *
   * @param query [[Map[String, Map[String, String]]] - source query
   * @return [[DataFrame]] - dataframe with index time fields
   */
  private def searchMap(query: Map[String, Map[String, String]]): DataFrame = {
    val (df, allExceptions) = query.foldLeft((spark.emptyDataFrame, List[Exception]())) {
      case (accum, item) =>
        log.debug(s"[SearchID:$searchId]Query is " + item)
        val modifiedQuery = getModifedQuery(item)
        val nItem = (item._1, item._2 + ("query" -> modifiedQuery))
        log.debug(s"[SearchID:$searchId]Modified query is" + nItem)

        val s = new IndexSearch(spark, log, nItem, searchId, Seq[String](), preview)
        try {
          // Read index data (only _time and _raw) and make field extraction
          val fdfe: DataFrame = extractFields(s.search())
          // If the index for some reason already contained an index field, it will be removed
          // because the value of the index field may differ from the name of the index directory
          val ifdfe = if (fieldsUsedInFullQuery.contains("index"))
            fdfe.drop("index").withColumn("index", lit(item._1))
          else
            fdfe


          val fdf :DataFrame = if (modifiedQuery == "") ifdfe else ifdfe.filter(modifiedQuery)
          val cols1 = fdf.columns.map(_.stripBackticks().addSurroundedBackticks).toSet
          val cols2 = accum._1.columns.map(_.stripBackticks().addSurroundedBackticks).toSet
          val totalCols = (cols1 ++ cols2).toList
          def expr(myCols: Set[String], allCols: Set[String]): List[Column] = {
            allCols.toList.map(x => if (myCols.contains(x)) F.col(x).as(x.stripBackticks()) else F.lit(null).as(x.stripBackticks()))
          }

          if (totalCols.nonEmpty) (fdf.select(expr(cols1, totalCols.toSet): _*).union(accum._1.select(expr(cols2, totalCols.toSet): _*)), accum._2)
          else accum
        } catch {
          case ex: Exception => (accum._1, ex +: accum._2)
        }
    }
    // Throw exception if for all index get errors
    if (query.size == allExceptions.size) throw allExceptions.head
    // Add columns which are used in query but does not exist in dataframe after read (as null values)
    val emptyCols = fieldsUsedInFullQuery
      .map(_.stripBackticks())
      .distinct
      .filterNot(_.contains("*"))
      .diff(df.columns.toSeq)
    log.debug(s"""[SearchID:$searchId] Add null cols to dataframe: [${emptyCols.mkString(", ")}]""")
    emptyCols.foldLeft(df) {
      (acc, col) => acc.withColumn(col, F.lit(null))
    }
    df
  }

  /**
   * Finds fields from the query that are not among the non-empty fields of the dataframe
   * Makes field extraction for these search-time-field-extraction Fields (by calling makeFieldExtraction)
   * Adds extracted columns to dataframe
   * Extracting algorithms are described in the FieldExtractor class
   *
   * @param df [[DataFrame]] - source dataframe
   * @return [[DataFrame]] - dataframe with search time fields
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
   * @return [[DataFrame]] - dataframe with search time fields
   */
  private def makeFieldExtraction(df: DataFrame, extractedFields: Seq[String], udf: UserDefinedFunction): DataFrame = {

    import org.apache.spark.sql.functions.{col, expr}

    val stfeFieldsStr = extractedFields.map(x => s""""${x.replaceAll("\\{(\\d+)}", "{}")}"""").mkString(", ")
    val mdf = df.withColumn("__fields__", expr(s"""array($stfeFieldsStr)"""))
      .withColumn("stfe", udf(col("_raw"), col("__fields__")))
    val fields: Seq[String] = if (extractedFields.exists(_.contains("*"))) {
      val sdf = mdf.agg(flatten(collect_set(map_keys(col("stfe")))).as("__schema__"))
      sdf.first.getAs[Seq[String]](0)
    } else extractedFields
    val existedFields = mdf.notNullColumns
    fields.foldLeft(mdf) { (acc, f) => {
      if (!existedFields.contains(f)) {
        if (f.contains("{}"))
          acc.withColumn(f, col("stfe")(f))
        else {
          val m = "\\{(\\d+)}".r.pattern.matcher(f)
          var index = if (m.find()) m.group(1).toInt - 1 else 0
          index = if (index < 0) 0 else index
          acc.withColumn(f, col("stfe")(f.replaceFirst("\\{\\d+}", "{}"))(index))
        }
      } else acc
    }
    }.drop("__fields__", "stfe")
  }

  /**
   * Standard method called by Converter in each OTL-command.
   *
   * @param _df [[DataFrame]] - incoming dataset (in generator-command like this one is ignored and should be empty)
   * @return [[DataFrame]]  - outgoing dataframe with OTL-command results
   */
  override def transform(_df: DataFrame): DataFrame = {
    log.debug(s"searchId = $searchId queryMap: $indexQueriesMap")
    val dfInit = searchMap(indexQueriesMap)
    val dfLimit = getKeyword("limit") match {
      case Some(lim) => log.debug(s"[SearchID:$searchId] Dataframe is limited to $lim"); dfInit.limit(100000)
      case _ => dfInit
    }
    // If subsearches exist, then we combine the dataframe with their results
    getKeyword("subsearch") match {
      case Some(str) =>
        cache.get(str) match {
          case Some(jdf) => new OTLJoin(SimpleQuery(s"""type=inner max=1 ${jdf.columns.toList.mkString(",")} subsearch=$str""", cache)).transform(dfLimit)
          case None => dfLimit
        }
      case None => dfLimit
    }
  }
}