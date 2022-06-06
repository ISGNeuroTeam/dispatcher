package ot.scalaotl

import org.apache.spark.sql.DataFrame

case class SimpleQuery(args: String, searchId: Int, cache: Map[String, DataFrame], subsearches: Map[String, String],
                       tws: Int, twf: Int, searchTimeFieldExtractionEnables: Boolean, preview: Boolean)

object SimpleQuery {
  def apply(args: String) = new SimpleQuery(args, -1, Map(), Map(), -1, -1, false, false)
  def apply(args: String, id: Int) = new SimpleQuery(args, id, Map(), Map(), -1, -1, false, false)
  def apply(args: String, cache: Map[String, DataFrame]) = new SimpleQuery(args, -1, cache, Map(), -1, -1, false, false)
}
