package ot.scalaotl
package utils
package rest

import ot.scalaotl.static.OtDatetime.{ getCurrentTimeInSeconds => currentTime }

import scalaj.http.Http
import scalaj.http.HttpResponse

import org.json4s._
import org.json4s.native.JsonMethods._

class RestJobLoader(sid: Int, query: String, tws: Int, twf: Int) extends RestJob(sid, query, tws, twf) {
  def this(q: String) = this(currentTime().toInt, q, 0, currentTime().toInt)
  def this() = this(currentTime().toInt, "", 0, currentTime().toInt)

  def getDataFromPostReqest(post: RestJobMaker) = {
    data = List("sid", "original_otl", "tws", "twf").foldLeft(data) {
      case (accum, item) => accum + (item -> post.data(item))
    }
    this
  }

  def get(endpoint: RestConnection, timeout: Int = 60): Option[HttpResponse[String]] = {
    implicit val formats = DefaultFormats
    val startTime = currentTime()
    def checkResponse: Option[HttpResponse[String]] = {
      val response = data.foldLeft(Http(endpoint.path)) {
        case (accum, (k, v)) => accum.param(k, v)
      }.asString
      val status = (parse(response.body) \ "status").extract[String]
      if ((status == "running") | (status == "new")) {
        if (currentTime() - startTime < timeout) checkResponse else None
      } else Some(response)
    }
    checkResponse
  }
}
