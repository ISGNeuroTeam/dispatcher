package ot.scalaotl.commands

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, functions => F}
import ot.scalaotl.Converter

class FullReadTest extends CommandTest {

  test("READ => TEST 1. Simple reading of some fields from index") {
    val otlQuery = createQuery(""" table _time, _raw, metric_name, value  """,
      "otstats", s"$test_index-0")
    val actual = new Converter(otlQuery).run
    val expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(F.col("_time"), F.col("_raw"), F.col("metric_name"), F.col("value"))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 2. Apply time range") {
    val otlQuery = createQuery(""" table _time, _raw, metric_name, value """,
      "otstats", s"$test_index-0", start_time, finish_time)
    val actual = new Converter(otlQuery).run
    val expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(F.col("_time"), F.col("_raw"), F.col("metric_name"), F.col("value"))
      .filter(F.col("_time").between(start_time, finish_time))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 3. Add empty cols") {
    val otlQuery = createQuery(""" eval a = if (new == 1, 1, -1) | table _time, metric_name, value, new, a""",
      "otstats", s"$test_index-0")
    val actual = new Converter(otlQuery).run
    val expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(F.col("_time"), F.col("metric_name"), F.col("value"))
      .withColumn("new", F.lit(null))
      .withColumn("a" , F.lit(-1))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 4. Reading of all indexes") {
    val query = createFullQuery(
      s"""search index=* | table _time, metric_name, value""",
      // s""" | otstats {"*": {"query": "", "tws": 0, "twf": 0}} | table _time, metric_name, value""",
      // due to the peculiarities of testing, it is necessary to form a list of available indices
      // (indices of other commands can be read during testing)
      s""" | otstats {"$test_index*": {"query": "", "tws": 0, "twf": 0}} | table _time, metric_name, value""",
      s"$test_index-0")
    var actual = new Converter(query).run
    var expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField("_time", LongType), StructField("metric_name", StringType),
        StructField("value", DoubleType))))
    for(i <- stfeRawSeparators.indices) {
      val df = readIndexDF(s"$test_index-$i", readingDatasetSchema)
        .select(F.col("_time"), F.col("metric_name"), F.col("value"))
      expected = df.union(expected)
    }
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 5. Reading indexes by name with wildcard") {
    val query = createFullQuery(
      s"""search index=$test_index* | table _time, metric_name, value""",
      s""" | otstats {"$test_index*": {"query": "", "tws": 0, "twf": 0}}  | table _time, metric_name, value""",
      s"$test_index-0")
    var actual = new Converter(query).run
    var expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField("_time", LongType), StructField("metric_name", StringType),
        StructField("value", DoubleType))))
    for(i <- stfeRawSeparators.indices) {
      val df = readIndexDF(s"$test_index-$i", readingDatasetSchema)
        .select(F.col("_time"), F.col("metric_name"), F.col("value"))
      expected = df.union(expected)
    }
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 6. Reading indexes by name, where one of indexes is not exist") {
    val non_existent_index = "non_existent_index"
    val query = createFullQuery(
      s"""search index=$test_index-0 index=$non_existent_index  table _time, metric_name, value""",
      s""" | otstats {"$test_index-0": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index": {"query": "", "tws": 0, "twf": 0}} | table _time, metric_name, value""",
      s"$test_index-0")
    var actual = new Converter(query).run
    var expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(F.col("_time"), F.col("metric_name"), F.col("value"))
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 7. Reading indexes by name, where all indexes are not exist") {
    val non_existent_index = "non_existent_index"
    val query = createFullQuery(
      s"""search index=$non_existent_index-1 index=$non_existent_index-2 | table _time, floor, room, metric_name, value, index, _raw""",
      s""" | otstats {"$non_existent_index-1": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index-2": {"query": "", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
      s"$test_index-0")
    val thrown = intercept[Exception] {
      new Converter(query).run
    }
    assert(thrown.getMessage.endsWith(s"Index not found: $non_existent_index-2"))
  }

  test("READ => TEST 8. Read fields from index with with field name wildcards") {
    val otlQuery = createQuery(""" table _time, metric*, *iption, *oo*  """,
      "otstats", s"$test_index-0")
    var actual = new Converter(otlQuery).run
    var expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("description"),
        F.col("metric_name"), F.col("metric_long_name"))
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 9.Read index with filter on existing fields") {
    val query = createFullQuery(
      s"""search index=$test_index-0 metric_name!="temperature_celsius" AnD NOT value>15.0 oR value>25.0  | table _time, floor, room, metric_name, value, index, _raw""",
      s""" | otstats {"$test_index-0": {"query": "!(metric_name=\\\"temperature_celsius\\\") AnD !(value>15.0) oR value>25.0", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
      s"$test_index-0")
    var actual = new Converter(query).run
    var expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .filter((!(F.col("metric_name") === "temperature_celsius") && (F.col("value") <= 15.0)) || F.col("value") > 25.0)
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
        F.col("value"), F.col("_raw"), F.col("index"))
    expected = setNullableStateOfColumn(expected, "index", nullable = true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => F.col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => F.col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 10.Read index with filter on not existing fields") {
    val query = createFullQuery(
      s"""search index=$test_index-0 metric!="temperature_celsius" AND NOT param>25.0 | table _time, floor, room, metric_name, value, index, _raw""",
      s""" | otstats {"$test_index-0": {"query": "!(metric=\\\"temperature_celsius\\\") AND !(param>25.0)", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
      s"$test_index-0")
    val thrown = intercept[Exception] {
      new Converter(query).run
    }
    assert(thrown.getMessage.contains("Error in  'fullread' command"))
  }

  test("READ => TEST 11. Read index with filter on not existing field with negation") {
    val query = createFullQuery(
      s"""search index=$test_index-0 'company.val'!=null | table _time, metric_name, value""",
      s""" | otstats {"$test_index-0": {"query": "'company.val'!=null", "tws": 0, "twf": 0}} | table _time, metric_name, value""",
      s"$test_index-0")
    val actual = new Converter(query).run
    val expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(
      Seq(StructField("_time", LongType), StructField("metric_name", StringType), StructField("value", DoubleType))))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 12. Read index with filter on not existing field with negation and {}") {
    val query = createFullQuery(
      s"""search index=$test_index-0 'text{5}.val'!=null | table _time, metric_name, value""",
      s""" | otstats {"$test_index-0": {"query": "'text{5}.val'!=null", "tws": 0, "twf": 0}} | table _time, metric_name, value""",
      s"$test_index-0")
    val actual = new Converter(query).run
    val expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(
      Seq(StructField("_time", LongType), StructField("metric_name", StringType), StructField("value", DoubleType))))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 13. Read index with filter on existing field with negation and {}") {
    val query = createFullQuery(
      s"""search index=$test_index-0 text=RUB 'text{2}.val'!=null | table _time, text, 'text{1}.val', 'text{2}.val'""",
      s""" | otstats {"$test_index-0": {"query": "text=\\\"RUB\\\" AND !('text{2}.val'=\\\"null\\\")", "tws": 0, "twf": 0}} | table _time, text, 'text{1}.val', 'text{2}.val'""",
      s"$test_index-0")
    val actual = new Converter(query).run
    val expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .filter(F.col("`text[2].val`").isNotNull)
      .filter(F.col("text").like("RUB"))
      .withColumnRenamed("text[1].val", "text{1}.val")
      .withColumnRenamed("text[2].val", "text{2}.val")
      .select(F.col("_time"), F.col("text"), F.col("`text{1}.val`"), F.col("`text{2}.val`"))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 14. Read index with filter on existing field with negation and {} using ()") {
    val query = createFullQuery(
      s"""search index=$test_index-0 (text=RUB 'text{2}.val'!=null) | table _time, text, 'text{1}.val', 'text{2}.val'""",
      s""" | otstats {"$test_index-0": {"query": "(text=\\\"RUB\\\" AND !('text{2}.val'=\\\"null\\\"))", "tws": 0, "twf": 0}} | table _time, text, 'text{1}.val', 'text{2}.val'""",
      s"$test_index-0")
    val actual = new Converter(query).run
    val expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .filter(F.col("`text[2].val`").isNotNull)
      .filter(F.col("text").like("RUB"))
      .withColumnRenamed("text[1].val", "text{1}.val")
      .withColumnRenamed("text[2].val", "text{2}.val")
      .select(F.col("_time"), F.col("text"), F.col("`text{1}.val`"), F.col("`text{2}.val`"))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 15. Create multi-value cols") {
    val otlQuery = createQuery("""fields _time, 'num{}.val', 'text{}.val' | eval res = mvzip('num{}.val', 'text{}.val') | table _time, res, num*, text*""",
      "otstats", s"$test_index-0")
    val actual = new Converter(otlQuery).run
    var expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(F.col("_time"),
        F.col("`text`"), F.col("`text[1].val`"), F.col("`text[2].val`"),
        F.col("`num`"), F.col("`num[1].val`"), F.col("`num[2].val`")
      )
      .withColumn("res", F.array(
        F.concat(F.col("`num[1].val`"), F.lit(' '), F.col("`text[1].val`")),
        F.concat(F.col("`num[2].val`"), F.lit(' '), F.col("`text[2].val`")))
      )
      .withColumn("num{}.val", F.array(F.col("`num[1].val`"), F.col("`num[2].val`")))
      .withColumn("text{}.val", F.array(F.col("`text[1].val`"), F.col("`text[2].val`")))
      .select(
        F.col("_time"),  F.col("res"), F.col("`num{}.val`"), F.col("`text{}.val`")
      )
    expected = setNullableStateOfColumn(expected, "res", nullable = true)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 16. Replace square brackets with curly brackets in col names") {
    val otlQuery = createQuery("""fields _time, 'num{1}.val', 'text{2}.val'""",
      "otstats", s"$test_index-0")
    val actual = new Converter(otlQuery).run
    val expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .withColumnRenamed("num[1].val", "num{1}.val")
      .withColumnRenamed("text[2].val", "text{2}.val")
      .select(F.col("_time"), F.col("`num{1}.val`"), F.col("`text{2}.val`"))
    compareDataFrames(actual, expected)
  }

}