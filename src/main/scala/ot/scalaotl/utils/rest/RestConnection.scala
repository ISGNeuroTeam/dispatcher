package ot.scalaotl
package utils
package rest

case class RestConnection(server: String, port: String, endpoint: String) {
  val path = s"http://${server}:${port}/${endpoint}"
}
