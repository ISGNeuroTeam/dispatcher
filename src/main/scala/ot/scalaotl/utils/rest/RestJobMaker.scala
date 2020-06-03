package ot.scalaotl
package utils
package rest

import ot.scalaotl.static.OtDatetime.{ getCurrentTimeInSeconds => currentTime }

import scalaj.http.Http
import scalaj.http.HttpResponse

class RestJobMaker(sid: Int, query: String, ttl: Int, tws: Int, twf: Int, username: String, preview: Boolean)
  extends RestJob(sid, query, tws, twf) {
  def this(q: String) = this(currentTime().toInt, q, 3600, 0, currentTime().toInt, "admin", false)
  def this(q: String, ttl: Int) = this(currentTime().toInt, q, ttl, 0, currentTime().toInt, "admin", false)

  option("cache_ttl", ttl.toString)
    .option("username", username)
    .option("preview", preview.toString)

  def post(endpoint: RestConnection): HttpResponse[String] = {
    Http(endpoint.path)
      .postForm(data.toSeq)
      .asString
  }
}