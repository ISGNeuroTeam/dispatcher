package ot.scalaotl
package utils
package rest

import ot.scalaotl.static.OtDatetime.{ getCurrentTimeInSeconds => currentTime }

class RestJob(sid: Int, query: String, tws: Int, twf: Int) {
  def this(sid: Long, q: String) = this(sid.toInt, q, 0, currentTime().toInt)
  def this(query: String) = this(currentTime().toInt, query, 0, currentTime().toInt)

  var data: Map[String, String] = Map(
    "sid" -> sid.toString,
    "original_otl" -> query,
    "tws" -> tws.toString,
    "twf" -> twf.toString)

  def option(name: String, value: String) = {
    data = data + (name -> value)
    this
  }
}