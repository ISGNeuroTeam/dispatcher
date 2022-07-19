package ot.scalaotl.commands

import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.{functions => F}
import ot.scalaotl.Converter

class OTLFieldsTest extends CommandTest {


  test("Test 1. Command: | Selection of existing fields") {
    val query = createQuery("""fields _time, _meta, host, sourcetype""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    val expected = readIndexDF(test_index)
      .select(F.col("_time"), F.col("_meta"), F.col("host"), F.col("sourcetype"))
    compareDataFrames(actual, expected)
  }

  test("Test 2. Command: | Selection of existing fields (with plus)") {
    val query = createQuery("""fields + _time, _meta, host, sourcetype""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    val expected = readIndexDF(test_index)
      .select(F.col("_time"), F.col("_meta"), F.col("host"), F.col("sourcetype"))
    compareDataFrames(actual, expected)
  }

  test("Test 3. Command: | Selection of a non-existent field") {
    val query = createQuery("""fields _time, meta, host, junk_Field""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    var expected = readIndexDF(test_index).select(F.col("_time"), F.col("host"))
      .withColumn("meta", F.lit(null).cast(NullType))
      .withColumn("junk_Field", F.lit(null).cast(NullType))
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("Test 4. Command: | Removing of existing fields") {
    val query = createQuery("""fields - _time, _meta, host, sourcetype""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    var expected = readIndexDF(test_index).drop(F.col("_time")).drop(F.col("_meta"))
      .drop(F.col("host")).drop(F.col("sourcetype"))
    expected = setNullableStateOfColumn(expected, "index", nullable = true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("Test 5. Command: | Removing of non-existing fields") {
    val query = createQuery("""fields - _time, meta, host, junk_Field""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    var expected = readIndexDF(test_index).drop(F.col("_time")).drop(F.col("host"))
    expected = setNullableStateOfColumn(expected, "index", nullable = true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("Test 6. Command: Selection fields by wildcard") {
    val query = createQuery("""fields _nifi_time*, *Field, _time""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    var expected = readIndexDF(test_index)
    val regex1 = ".*Field".r
    val regex2 = "_nifi_time*".r
    val selection = expected.columns.filter(s => regex1.findFirstIn(s).isDefined || regex2.findFirstIn(s).isDefined
      || s.equals("_time"))
    expected = expected.select(selection.head, selection.tail : _*)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }


  test("Test 7. Command: Selection of all fields") {
    val query = createQuery("""fields *""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    var expected = readIndexDF(test_index)
    expected = setNullableStateOfColumn(expected, "index", nullable = true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("Test 8. Command: Selection of all fields") {
    val query = createQuery("""fields + *""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    var expected = readIndexDF(test_index)
    expected = setNullableStateOfColumn(expected, "index", nullable = true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("Test 9. Command: Removing of all fields") {
    val query = createQuery("""fields - *""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    compareDataFrames(actual, spark.emptyDataFrame)
  }


  test("Test 10. Command: | Query without field-list") {
    val query = createQuery("""fields""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    compareDataFrames(actual, spark.emptyDataFrame)
  }

  test("Test 11. Command: | Query without field-list") {
    val query = createQuery("""fields +""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    compareDataFrames(actual, spark.emptyDataFrame)
  }

  test("Test 12. Command: | Query without field-list") {
    val query = createQuery("""fields -""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    var expected = readIndexDF(test_index)
    expected = setNullableStateOfColumn(expected, "index", nullable = true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("Test 13. Command: | Selection of existing fields with wrong plus and minus") {
    val query = createQuery("""fields _time, +, _meta, -, host, sourcetype""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    val expected = readIndexDF(test_index)
      .select(F.col("_time"), F.col("_meta"), F.col("host"), F.col("sourcetype"))
    compareDataFrames(actual, expected)
  }


  test("Test 14. Command: | Another incorrect query") {
    val query = createQuery("""fields + +""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    compareDataFrames(actual, spark.emptyDataFrame)
  }

  test("Test 15. Command: | Another incorrect query") {
    val query = createQuery("""fields - -""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    var expected = readIndexDF(test_index)
    expected = setNullableStateOfColumn(expected, "index", nullable = true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("Test 17. Command: | Another incorrect query") {
    val query = createQuery("""fields * +""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    var expected = readIndexDF(test_index)
    expected = setNullableStateOfColumn(expected, "index", nullable = true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("Test 18. Command: | Another incorrect query") {
    val query = createQuery("""fields * -""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    var expected = readIndexDF(test_index)
    expected = setNullableStateOfColumn(expected, "index", nullable = true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }


}
