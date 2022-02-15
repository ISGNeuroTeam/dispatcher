package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.json4s._
import org.json4s.native.JsonMethods._

class OTLFilter(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("subsearch")
  val cache: Map[String, DataFrame] = sq.cache

  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def jsonStrToMap(jsonStr: String): JValue = {
    parse(jsonStr)
  }

  val json: JValue = jsonStrToMap(args.trim)
  val queryMap: String = (json \ "query").extract[String]
  override val fieldsUsed: List[String] = (json \ "fields").extract[List[String]]

  // ==== without possible subsearch with return command ====
  // override def transform(_df: DataFrame): DataFrame = {
  //   val query = queryMap("query")
  //   if (query.isEmpty) return _df
  //   _df.filter(query)
  // }

  override def transform(_df: DataFrame): DataFrame = {
    val initDf = if (queryMap.isEmpty) _df else _df.filter(queryMap)
    getKeyword("subsearch") match {
      case Some(str) =>
        cache.get(str) match {
          case Some(jdf) => new OTLJoin(SimpleQuery(s"""type=inner max=1 ${jdf.columns.toList.mkString(",")} subsearch=$str""", cache)).transform(initDf)
          case None => initDf
        }
      case None => initDf
    }
  }
}
