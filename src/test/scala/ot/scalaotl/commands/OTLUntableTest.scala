package ot.scalaotl.commands

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{functions => F}
import ot.scalaotl.Converter

class OTLUntableTest extends CommandTest {

  test("Test 1. Command: Convert wide table to long table ") {
    val query = createQuery(
      """table WordField ,serialField, random_Field
        || untable WordField, field, value""", "otstats", s"$test_index")
    val actual = new Converter(query).run
    val expected = """[
                     |{"WordField":"qwe","field":"serialField","value":"0"},
                     |{"WordField":"qwe","field":"random_Field","value":"100"},
                     |{"WordField":"rty","field":"serialField","value":"1"},
                     |{"WordField":"rty","field":"random_Field","value":"-90"},
                     |{"WordField":"uio","field":"serialField","value":"2"},
                     |{"WordField":"uio","field":"random_Field","value":"50"},
                     |{"WordField":"GreenPeace","field":"serialField","value":"3"},
                     |{"WordField":"GreenPeace","field":"random_Field","value":"20"},
                     |{"WordField":"fgh","field":"serialField","value":"4"},
                     |{"WordField":"fgh","field":"random_Field","value":"30"},
                     |{"WordField":"jkl","field":"serialField","value":"5"},
                     |{"WordField":"jkl","field":"random_Field","value":"50"},
                     |{"WordField":"zxc","field":"serialField","value":"6"},
                     |{"WordField":"zxc","field":"random_Field","value":"60"},
                     |{"WordField":"RUS","field":"serialField","value":"7"},
                     |{"WordField":"RUS","field":"random_Field","value":"-100"},
                     |{"WordField":"MMM","field":"serialField","value":"8"},
                     |{"WordField":"MMM","field":"random_Field","value":"0"},
                     |{"WordField":"USA","field":"serialField","value":"9"},
                     |{"WordField":"USA","field":"random_Field","value":"10"}
                     |]""".stripMargin
    compareDataFrames(actual, jsonToDf(expected))
  }

  test("Test 2. Command: Fixed column missing among data fields") {
    val query = createQuery("""table serialField, random_Field
                              || untable WordField, field, value""", "otstats", s"$test_index")
    var actual = new Converter(query).run
    val expectedStr = """[
                        |{"field":"serialField","value":"0"},
                        |{"field":"random_Field","value":"100"},
                        |{"field":"serialField","value":"1"},
                        |{"field":"random_Field","value":"-90"},
                        |{"field":"serialField","value":"2"},
                        |{"field":"random_Field","value":"50"},
                        |{"field":"serialField","value":"3"},
                        |{"field":"random_Field","value":"20"},
                        |{"field":"serialField","value":"4"},
                        |{"field":"random_Field","value":"30"},
                        |{"field":"serialField","value":"5"},
                        |{"field":"random_Field","value":"50"},
                        |{"field":"serialField","value":"6"},
                        |{"field":"random_Field","value":"60"},
                        |{"field":"serialField","value":"7"},
                        |{"field":"random_Field","value":"-100"},
                        |{"field":"serialField","value":"8"},
                        |{"field":"random_Field","value":"0"},
                        |{"field":"serialField","value":"9"},
                        |{"field":"random_Field","value":"10"}
                        |]""".stripMargin
    var expected = jsonToDf(expectedStr).withColumn("WordField", F.lit(null))
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("Test 3. Command: Fixed column contains nulls") {
    val query = createQuery("""table serialField, random_Field
                              | table WordField ,serialField, random_Field
                              | untable WordField, field, value""", "otstats", s"$test_index")
    var actual = new Converter(query).run
    val expectedStr = """[
                        |{"field":"serialField","value":"0"},
                        |{"field":"random_Field","value":"100"},
                        |{"field":"serialField","value":"1"},
                        |{"field":"random_Field","value":"-90"},
                        |{"field":"serialField","value":"2"},
                        |{"field":"random_Field","value":"50"},
                        |{"field":"serialField","value":"3"},
                        |{"field":"random_Field","value":"20"},
                        |{"field":"serialField","value":"4"},
                        |{"field":"random_Field","value":"30"},
                        |{"field":"serialField","value":"5"},
                        |{"field":"random_Field","value":"50"},
                        |{"field":"serialField","value":"6"},
                        |{"field":"random_Field","value":"60"},
                        |{"field":"serialField","value":"7"},
                        |{"field":"random_Field","value":"-100"},
                        |{"field":"serialField","value":"8"},
                        |{"field":"random_Field","value":"0"},
                        |{"field":"serialField","value":"9"},
                        |{"field":"random_Field","value":"10"}
                        |]""".stripMargin
    var expected = jsonToDf(expectedStr).withColumn("WordField", F.lit(null))
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("Test 3. Command: Call untable with 2 arguments ") {
    val query = createQuery("""table WordField ,serialField, random_Field
                              || untable WordField, field""", "otstats", s"$test_index")
    val actual = new Converter(query).run
    compareDataFrames(actual, spark.emptyDataFrame)
  }

}
