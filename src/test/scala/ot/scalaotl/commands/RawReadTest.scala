package ot.scalaotl.commands


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, functions => F}
import ot.scalaotl.Converter

class RawReadTest extends CommandTest {

  test("READ => TEST 1. Simple read only _time and _raw fields") {
    val otlQuery = createQuery("", "read", s"$test_index-0")
    val actual = new Converter(otlQuery).run
    val expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(F.col("_time"), F.col("_raw"))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 2. Apply time range") {
    val otlQuery = createQuery("", "read", s"$test_index-0", start_time, finish_time)
    val actual = new Converter(otlQuery).run
    val expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(F.col("_time"), F.col("_raw"))
      .filter(F.col("_time").between(start_time, finish_time))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 3. Add empty cols") {
    val otlQuery = createQuery("""eval a = if (new == 1, 1, -1) | table _time, new, a""",
      "read", s"$test_index-0")
    val actual = new Converter(otlQuery).run
    val expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(F.col("_time"))
      .withColumn("new", F.lit(null).cast(StringType))
      .withColumn("a" , F.lit(-1))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 4. Reading of all indexes") {
    val indexList =
      for (i <- stfeRawSeparators.indices)
        yield s""" "$test_index-$i": {"query": "", "tws": 0, "twf": 0}"""
    val serviceOtl = indexList.mkString(",")
    val query = createFullQuery(
      s"""search index=*""",
      s"""| read {$serviceOtl}""",
      s"$test_index-0")
    var actual = new Converter(query).run
    var expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField("_time", LongType), StructField("_raw", StringType))))
    for(i <- stfeRawSeparators.indices) {
      val df = readIndexDF(s"$test_index-$i", readingDatasetSchema)
        .select(F.col("_time"), F.col("_raw"))
      expected = df.union(expected)
    }
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 5. Reading indexes by name with wildcard") {
    val query = createFullQuery(
      s"""search index=$test_index*""",
      s"""| read {"$test_index*": {"query": "", "tws": 0, "twf": 0}}""",
      s"$test_index-0")
    var actual = new Converter(query).run
    var expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField("_time", LongType), StructField("_raw", StringType))))
    for(i <- stfeRawSeparators.indices) {
      val df = readIndexDF(s"$test_index-$i").select(F.col("_time"), F.col("_raw"))
      expected = df.union(expected)
    }
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 6. Read indexes with wildcard. One of indexes is not exist ") {
    val non_existent_index = "non_existent_index"
    val query = createFullQuery(
      s"""search index=$test_index-1 index=$non_existent_index""",
      s"""| read {"$test_index-1": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index": {"query": "", "tws": 0, "twf": 0}}""",
      s"$test_index-0")
    val actual = new Converter(query).run
    val expected = readIndexDF(s"$test_index-1", readingDatasetSchema)
      .select(F.col("_time"), F.col("_raw"))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 7. Read indexes with wildcard. All indexes is not exist ") {
    val non_existent_index = "non_existent_index"
    val query = createFullQuery(
      """search index=$non_existent_index-1 index=$non_existent_index-2""",
      s""" | read {"$non_existent_index-1": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index-2": {"query": "", "tws": 0, "twf": 0}}""",
      s"$test_index-0")
    val thrown = intercept[Exception] {
      new Converter(query).run
    }
    assert(thrown.getMessage.endsWith(s"Index not found: $non_existent_index-2"))
  }

  test("READ => TEST 8. Read fields from index with with field name wildcards") {
    val otlQuery = createQuery(""" table _time, metric*, *iption, *oo*  """,
      "read", s"$test_index-0")
    var actual = new Converter(otlQuery).run
    var expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("description"),
        F.col("metric_name"), F.col("metric_long_name")
      )
      .withColumn("floor", col("floor").cast(StringType))
      .withColumn("room", col("room").cast(StringType))
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 9.Read index with filter on existing fields") {
    val query = createFullQuery(
      s"""search index=$test_index-0 metric_name!="temperature_celsius" | table _time, floor, room, metric_name, value, index, _raw""",
      s""" | read {"$test_index-0": {"query": "!(metric_name=\\\"temperature_celsius\\\")", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
      s"$test_index-0")
    var actual = new Converter(query).run
    actual = actual.withColumn("value", col("value").cast(DoubleType))
    var expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .filter(!(F.col("metric_name") === "temperature_celsius"))
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
        F.col("value"), F.col("_raw"), F.col("index"))
      .withColumn("floor", col("floor").cast(StringType))
      .withColumn("room", col("room").cast(StringType))
    expected = setNullableStateOfColumn(expected, "index", nullable = true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 10.Read index with filter on not existing fields") {
    val query = createFullQuery(
      s"""search index=$test_index-0 metric!="temperature_celsius" | table _time, metric""",
      s""" | read {"$test_index-0": {"query": "!(metric=\\\"temperature_celsius\\\")", "tws": 0, "twf": 0}}  | table _time, metric""",
      s"$test_index-0")
    var actual = new Converter(query).run
    var expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField("_time", LongType), StructField("metric", StringType))))
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 11. Read index with filter on not existing field with negation") {
    val query = createFullQuery(
      s"""search index=$test_index-0 'company.val'!=null | table _time, metric_name, value""",
      s""" | read {"$test_index-0": {"query": "'company.val'!=null", "tws": 0, "twf": 0}} | table _time, metric_name, value""",
      s"$test_index-0")
    var actual = new Converter(query).run
    actual = actual.withColumn("value", col("value").cast(DoubleType))
    val expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(
      Seq(StructField("_time", LongType), StructField("metric_name", StringType), StructField("value", DoubleType))))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 12. Read index with filter on not existing field with negation and {}") {
    val query = createFullQuery(
      s"""search index=$test_index-0 'text{5}.val'!=null | table _time, metric_name, value""",
      s""" | read {"$test_index-0": {"query": "'text{5}.val'!=null", "tws": 0, "twf": 0}} | table _time, metric_name, value""",
      s"$test_index-0")
    var actual = new Converter(query).run
    actual = actual.withColumn("value", col("value").cast(DoubleType))
    val expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(
      Seq(StructField("_time", LongType), StructField("metric_name", StringType), StructField("value", DoubleType))))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 13. Read index with filter on existing field with negation and {}") {
    val query = createFullQuery(
      s"""search index=$test_index-0 text=RUB 'text{2}.val'!=null | table _time, text, 'text{1}.val', 'text{2}.val'""",
      s""" | read {"$test_index-0": {"query": "text=\\\"RUB\\\" AND !('text{2}.val'=\\\"null\\\")", "tws": 0, "twf": 0}} | table _time, text, 'text{1}.val', 'text{2}.val'""",
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
      s""" | read {"$test_index-0": {"query": "(text=\\\"RUB\\\" AND !('text{2}.val'=\\\"null\\\"))", "tws": 0, "twf": 0}} | table _time, text, 'text{1}.val', 'text{2}.val'""",
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
      "read", s"$test_index-0")
    val actual = new Converter(otlQuery).run
    var expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(
        F.col("_time"),
        F.col("`text`"), F.col("`text[1].val`"), F.col("`text[2].val`"),
        F.col("`num`"), F.col("`num[1].val`"), F.col("`num[2].val`")
      )
      .withColumn("num", F.col("num").cast(StringType))
      .withColumn("num[1].val", F.col("`num[1].val`").cast(StringType))
      .withColumn("num[2].val", F.col("`num[2].val`").cast(StringType))
      .withColumn("res", array(
        F.concat(F.col("`num[1].val`"), F.lit(' '), F.col("`text[1].val`")),
        F.concat(F.col("`num[2].val`"), F.lit(' '), F.col("`text[2].val`")))
      )
      .withColumn("num{}.val", array(F.col("`num[1].val`"), F.col("`num[2].val`")))
      .withColumn("text{}.val", array(F.col("`text[1].val`"), F.col("`text[2].val`")))
      .select(
        F.col("_time"),  F.col("res"), F.col("`num{}.val`"), F.col("`text{}.val`")
      )
    expected = expected.sqlContext.createDataFrame(expected.rdd, StructType(expected.schema.map(_.copy(nullable = true))))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 16. Create multi-value cols") {
    val otlQuery = createQuery("""fields _time, listField{}, 'nestedField{}.val'""",
      "read", s"$test_index-0")
    val actual = new Converter(otlQuery).run
    val schema = ArrayType(StructType(Array(StructField("val", StringType, nullable = false))), containsNull = false)
    var expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .select(F.col("_time"), F.col("listField"), F.col("nestedField"))
      .withColumn("listField{}", F.split(regexp_replace(F.col("listField"), "\\[|\\]", ""), ", "))
      .withColumn("nestedField{}.val", F.from_json(F.col("nestedField"), schema).getField("val"))
      .select(F.col("_time"), F.col("`listField{}`"), F.col("`nestedField{}.val`"))
    expected = expected.sqlContext.createDataFrame(expected.rdd, StructType(expected.schema.map(_.copy(nullable = true))))
    compareDataFrames(actual, expected)
  }


  test("READ => TEST 17. Replace square brackets with curly brackets in col names") {
    val otlQuery = createQuery("""fields _time, 'num{1}.val', 'text{2}.val'""",
      "read", s"$test_index-0")
    val actual = new Converter(otlQuery).run
    val expected = readIndexDF(s"$test_index-0", readingDatasetSchema)
      .withColumn("num[1].val", F.col("`num[1].val`").cast(StringType))
      .withColumnRenamed("num[1].val", "num{1}.val")
      .withColumnRenamed("text[2].val", "text{2}.val")
      .select(F.col("_time"), F.col("`num{1}.val`"), F.col("`text{2}.val`"))
    compareDataFrames(actual, expected)
  }

  def stfeExtractionTest(k: Int): Unit ={
    val query = createFullQuery(
      s"""search index=$test_index-$k | table _time, _raw, metric_name, metric_long_name, value | sort _time, metric_name, value""",
      s"""| read {"$test_index-$k": {"query": "", "tws": $start_time, "twf": $finish_time}} | table _time, _raw, metric_name, metric_long_name, value""",
      s"$test_index-$k", start_time, finish_time)
    var actual = new Converter(query).run
    actual = actual.withColumn("value", col("value").cast(DoubleType))
    val expected = readIndexDF(s"$test_index-$k", readingDatasetSchema)
      .filter(F.col("_time").between(start_time, finish_time))
      .select(
        F.col("_time"), F.col("_raw"),
        F.col("metric_name"), F.col("metric_long_name"), F.col("value")
      )
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 18. Search-time field extraction. Extracting fields from json") {
    stfeExtractionTest(0)
  }

  test("READ => TEST 19. Search-time field extraction. Extracting fields from _raw") {
    stfeExtractionTest(1)
  }

  test("READ => TEST 20. Search-time field extraction. Extracting fields from _raw") {
    stfeExtractionTest(2)
  }

  test("READ => TEST 21. Search-time field extraction. Extracting fields from _raw") {
    stfeExtractionTest(3)
  }

  test("READ => TEST 22. Search-time field extraction. Extracting fields from _raw") {
    stfeExtractionTest(4)
  }

  test("READ => TEST 23. Search-time field extraction. Extracting fields from _raw") {
    stfeExtractionTest(5)
  }

  test("READ => TEST 24. Search-time field extraction. Extracting fields from _raw") {
    stfeExtractionTest(6)
  }

  test("READ => TEST 25. Search-time field extraction. Extracting fields from _raw") {
    stfeExtractionTest(7)
  }

  test("READ => TEST 26. Search-time field extraction. Extracting fields from _raw") {
    stfeExtractionTest(8)
  }

}