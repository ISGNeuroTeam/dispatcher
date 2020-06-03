package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import org.json4s._
import org.json4s.native.JsonMethods._

class OTLFilter(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("subsearch")
  val cache = sq.cache

  implicit val formats = org.json4s.DefaultFormats

  def jsonStrToMap(jsonStr: String): JValue = {
    parse(jsonStr)
  }

  val json = jsonStrToMap(args.trim)
  val queryMap = (json \ "query").extract[String]
  override val fieldsUsed = (json \ "fields").extract[List[String]]

  // ==== without possible subsearch with return command ====
  // override def transform(_df: DataFrame): DataFrame = {
  //   val query = queryMap("query")
  //   if (query.isEmpty) return _df
  //   _df.filter(query)
  // }

  override def transform(_df: DataFrame): DataFrame = {
    val initDf = if (queryMap.toString.isEmpty) _df else _df.filter(queryMap.toString)
    getKeyword("subsearch") match {
      case Some(str) => {
        cache.get(str) match {
          case Some(jdf) => new OTLJoin(SimpleQuery(s"""type=inner max=1 ${jdf.columns.toList.mkString(",")} subsearch=$str""", cache)).transform(initDf)
          case None      => initDf
        }
      }
      case None => initDf
    }
  }
}
