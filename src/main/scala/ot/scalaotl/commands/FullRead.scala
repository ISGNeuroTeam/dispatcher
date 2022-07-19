package ot.scalaotl
package commands

import ot.scalaotl.utils.searchinternals._
import ot.scalaotl.config.OTLIndexes
import ot.scalaotl.extensions.StringExt._
import ot.scalaotl.extensions.DataFrameExt._
import ot.scalaotl.parsers.{ExpressionParser, WildcardParser}
import org.apache.spark.sql.{Column, DataFrame, functions => F}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable.ListBuffer

/** =Abstract=
 * This class provides support of __'''otstats'''__ otl command.
 *
 * __'''search'''__ used for read data from indexes (any fields)
 * The command can read data from multiple indexes (in this case indexes will be combined with logical OR)
 * The command can filter data by given field values (also they can be combined via logical operations AND and OR)
 * Logical operations names (AND and OR) are not case sensitive
 * Index names and field names can use wildcards
 *
 * =Usage example=
 * OTL: read one index (all fields)
 * {{{ otstats index="index_name" metric_name | ... other otl-commands }}}
 *
 * OTL: read two indexes, one with wildcard (all fields)
 * {{{ otstats index="first_index" index="second_ind*" | ... other otl-commands }}}
 *
 * OTL: read from all indexes
 * {{{ otstats index="*" | ... other otl-commands }}}
 *
 * OTL: read one index with filter on field
 * {{{ otstats index="index_name" metric_name="temperature" | ... other otl-commands }}}
 *
 * OTL: read one index with filter on two fields, one with wildcard (be careful with AND OR priorities)
 * {{{ otstats index="index_name" metric_name="temp*" AND value<20.0 | ... other otl-commands }}}
 *
 * OTL: read one index with complex boolean expression for filter on two fields
 * {{{ otstats index="index_name" metric_name="temp*" AND value<20.0 OR value>50.0 | ... other otl-commands }}}
 *
 * @constructor creates new instance of [[ FullRead ]]
 * @param sq [[ SimpleQuery ]] - contains args, cache, subsearches, search time interval, stfe and preview flags
 */
class FullRead(sq: SimpleQuery) extends OTLBaseCommand(sq) with OTLIndexes with ExpressionParser with WildcardParser {
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
  val allIndexes: ListBuffer[String] = getAllIndexes
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


  /**
   * Replace all {} symbols to [] in fieldnames in query.
   * Then replaces all quotes around fieldnames to bacticktics.
   * Then replaces fieldname=\"null\" substrings to 'fieldname is null'
   * Fieldnames are taken from `fieldsUsedInFullQuery`
   *
   * @param i [[ String, Map[String, String] ]] - item with original query
   * @return [[ String, Map[String, String] ]] - modified item
   */
  def modifyItemQuery(i: (String, Map[String, String])): (String, Map[String, String]) = {
    val query = i._2.getOrElse("query", "")
    val backtickedQuery = fieldsUsedInFullQuery
      // Filtering not number columns names (does not occur in RawRead because there are only columns _time and _raw)
      .filter(!_.matches("""^[0-9]*$"""))
      .foldLeft(query)((q, field) => {
        val nf = field.replace("{", "[").replace("}", "]").addSurroundedBackticks
        // Here a regular expression is used instead of a simple replacement,
        // because the name of one field can be a substring of another field
        // Starts with a bracket or space, then a quotation mark or backtick
        q.replaceAll("""([\(| ])(['`]*\Q""" + field + """\E['`]*)\s*(=|>|<|!=| like| rlike)""", s"$$1$nf$$3")
          .replace(nf + "=\"null\"", s"$nf is null")
          .replace(nf + "!=\"null\"", s"$nf is not null")
      })
    (i._1, i._2 + ("query" -> backtickedQuery))
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
        val mItem = modifyItemQuery(item)
        log.debug(s"[SearchID:$searchId]Query is " + item)
        log.debug(s"[SearchID:$searchId]Modified query is" + mItem)
        fieldsUsedInFullQuery = (item._2.get("query") match {
          case Some(x) if x != "" => getFieldsFromExpression(F.expr(x).expr, List()) ++ fieldsUsedInFullQuery
          case Some(_) | None => fieldsUsedInFullQuery
        }).distinct

        val s = new IndexSearch(spark, mItem, searchId, fieldsUsedInFullQuery, preview, true)
        try {
          // Read index data
          val fdf: DataFrame = s.search()

          log.debug(s"[SearchID:$searchId] fdf.schema: ${fdf.schema} ")
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

    // Add new columns with arrays of fields by mask ..[\d]...
    // Replace square brackets to curly brackets in column names
    val bracketCols = df.columns.filter("""^.*\[\d+].*$""".r.pattern.matcher(_).matches)
    val dfWithArrays = bracketCols
      .groupBy(_.replaceAll("\\[\\d+]", "{}"))
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
        if (fieldsUsedInFullQuery.contains(col.replaceByMap(Map("[" -> "{", "]" -> "}"))))
          acc.withSafeColumnRenamed(
            col, col.replaceByMap(Map("[" -> "{", "]" -> "}"))
          ) else acc.drop(col)
    }
    log.debug(s"""[SearchID:$searchId] DF cols after adding mv-columns: [${dfColsRenamed.columns.mkString(", ")}]""")
    dfColsRenamed
  }

  /**
   * Standard method called by Converter in each OTL-command.
   *
   * @param _df [[DataFrame]] - incoming dataset (in generator-command like this one is ignored and should be empty)
   * @return [[DataFrame]]  - outgoing dataframe with OTL-command results
   */
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
