package ot.scalaotl.commands

import ot.scalaotl.Converter
import org.apache.spark.sql.{functions => F}

class OTLCommentTest extends CommandTest {

  test("Test 1. Command: | -- new comment ") {
    val query = createQuery("""fields _time, _raw | -- new long comment""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    val expected = readIndexDF(test_index).select(F.col("_time"), F.col("_raw"))
    compareDataFrames(actual, expected)
  }

  test("Test 1. Command: | ___ new comment ") {
    val query = createQuery("""fields _time, _raw | ___ new long comment""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    val expected = readIndexDF(test_index).select(F.col("_time"), F.col("_raw"))
    compareDataFrames(actual, expected)
  }

  test("Test 1. Command: | --- FAIL new comment ") {
    val query = createQuery("""fields _time, _raw | --- new long comment""",
      "otstats", s"$test_index")

    val thrown = intercept[Exception] {
      val actual = new Converter(query).run
    }
    assert(thrown.getMessage.contains("new long comment"))
  }
}
