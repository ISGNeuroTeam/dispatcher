package ot.scalaotl.commands

import org.scalatest.Ignore
import ot.dispatcher.OTLQuery
import ot.scalaotl.Converter

@Ignore
class ConverterTest extends CommandTest {

  def createQuery(originalQuery: String, serviceQuery: String): OTLQuery = {
    new OTLQuery(
      id = 0,
      original_otl = originalQuery,
      service_otl = serviceQuery,
      tws = 0,
      twf = 0,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = false,
      preview = false
    )
  }

  test("Test 0. Mystic columns. Command sort.") {
    val query = "| makeresults count=10 | streamstats count as serialField | sort count=3 -serialField"
    val otlQuery = createQuery(query, query)
    val rdf = new Converter(otlQuery).run
    rdf.printSchema()
    rdf.show()
    val actual = rdf.toJSON.collect().mkString("[\n",",\n","\n]")
    val expected =
      """{}""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Mystic columns. Command head.") {
    val query = "| makeresults count=10 | streamstats count as serialField | head 3"
    val otlQuery = createQuery(query, query)
    val rdf = new Converter(otlQuery).run
    rdf.printSchema()
    rdf.show()
    val actual = rdf.toJSON.collect().mkString("[\n",",\n","\n]")
    val expected =
      """{}""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}