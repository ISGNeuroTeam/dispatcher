package ot.scalaotl.commands

import ot.dispatcher.OTLQuery
import ot.scalaotl.Converter

class OTLInputlookupTest extends CommandTest {

  override def createQuery(command_otl: String): OTLQuery = {
    new OTLQuery(
      id = 0,
      original_otl = s"|$command_otl",
      service_otl = "| " + command_otl,
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

  override def execute(query: String): String = {
    val otlQuery = createQuery(query)
    val df = new Converter(otlQuery).run
    df.toJSON.collect().mkString("[\n", ",\n", "\n]")
  }

  test("Test 0. Command: | inputlookup testlookup0.csv") {
    val lookup =
      """col1,col2
        |a,1
        |b,2""".stripMargin
    writeTextFile(lookup, "lookups/testlookup0.csv")

    val actual = execute("""inputlookup testlookup0.csv""")
    val expected =
      """[
        |{"col1": "a","col2": 1},
        |{"col1": "b","col2": 2}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | inputlookup testlookup1.csv where col2>1") {
    val lookup =
      """col1,col2
        |a,1
        |b,2""".stripMargin
    writeTextFile(lookup, "lookups/testlookup1.csv")

    val actual = execute("""inputlookup testlookup1.csv where {"query": "col2>1"}""")
    val expected =
      """[
        |{"col1": "b","col2": 2}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | inputlookup testlookup2.1.csv | inputlookup testlookup2.2.csv append=true") {
    val lookup1 =
      """col1,col2
        |a,1
        |b,2""".stripMargin
    writeTextFile(lookup1, "lookups/testlookup2.1.csv")
    val lookup2 =
      """col1,col2
        |c,3
        |d,4""".stripMargin
    writeTextFile(lookup2, "lookups/testlookup2.2.csv")

    val actual = execute("""| inputlookup testlookup2.1.csv | inputlookup testlookup2.2.csv append=t""")
    val expected =
      """[
        |{"col1": "a","col2": 1},
        |{"col1": "b","col2": 2},
        |{"col1": "c","col2": 3},
        |{"col1": "d","col2": 4}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | inputlookup testlookup.csv | eval t = t + m") {
    val lookup =
      """col1,col2
        |a,1
        |b,2""".stripMargin
    writeTextFile(lookup, "lookups/testlookup.csv")

    val actual = execute("""inputlookup testlookup.csv | eval t = t + m""")
    val expected =
      """[
        |{"col1": "a","col2": 1},
        |{"col1": "b","col2": 2}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4. Command: | inputlookup testlookup.csv | table col1,col2,col3") {
    val lookup =
      """col1,col2
        |a,1
        |b,2""".stripMargin
    writeTextFile(lookup, "lookups/testlookup.csv")

    val actual = execute("""inputlookup testlookup.csv | table col1,col2,col3""")
    val expected =
      """[
        |{"col1": "a","col2": 1},
        |{"col1": "b","col2": 2}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
