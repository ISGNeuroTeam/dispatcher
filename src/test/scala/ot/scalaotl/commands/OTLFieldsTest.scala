package ot.scalaotl.commands

import org.apache.spark.sql.{functions => F}
import ot.scalaotl.Converter

class OTLFieldsTest extends CommandTest {


  test("Test 1. Command: | head 1") {
    val query = createQuery("""fields _time, _meta, host, sourcetype""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    val expected = readIndexDF(test_index)
      .select(F.col("_time"), F.col("_meta"), F.col("host"), F.col("sourcetype"))
    compareDataFrames(actual, expected)
  }

  test("Test 0. Command: | head 1") {
    val query = createQuery("""fields - _raw""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    var expected = readIndexDF(test_index).drop(F.col("_raw"))
    expected = setNullableStateOfColumn(expected, "index", nullable = true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

}
