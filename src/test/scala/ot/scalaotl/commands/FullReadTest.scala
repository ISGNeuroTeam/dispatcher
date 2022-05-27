package ot.scalaotl.commands

import java.io.File
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types.{ArrayType, LongType, NullType, StringType, StructField, StructType}
import ot.dispatcher.OTLQuery
import ot.scalaotl.Converter


class FullReadTest extends CommandTest {

  override def createQuery(command_otl: String, tws: Int = 0, twf: Int = 0): OTLQuery ={
    val otlQuery = new OTLQuery(
      id = 0,
      original_otl = s"search index=$test_index-1 | $command_otl",
      service_otl = s""" | otstats {"$test_index-1": {"query": "", "tws": "$tws", "twf": "$twf"}} |  $command_otl """,
      tws = tws,
      twf = twf,
      cache_ttl = 0,
      indexes = Array(s"$test_index-1"),
      subsearches = Map(),
      username = "admin",
      field_extraction = false,
      preview = false
    )
    log.debug(s"otlQuery: $otlQuery.")
    otlQuery
  }

  test("READ => TEST 1. Simple correct read _time and _raw") {
    val otlQuery = createQuery(""" table _time, _raw  """)
    val actual = new Converter(otlQuery).run
    val expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors-1.csv")
      .withColumn("index", F.lit(f"$test_index-1"))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"), F.col("_raw"))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 2. Apply time range") {
    val start_time = 1649145660
    val finish_time = 1663228860
    val otlQuery = createQuery(""" fields _time, _raw """, start_time, finish_time)
    val actual = new Converter(otlQuery).run

    val expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors-1.csv")
      .withColumn("index", F.lit(f"$test_index-1"))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"), F.col("_raw"))
      .filter(F.col("_time").between(start_time, finish_time))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 3. Add empty cols") {
    val otlQuery = createQuery(""" eval a = if (new == 1, 1, -1) | table _time, new, a""")
    val actual = new Converter(otlQuery).run

    val expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors-1.csv")
      .withColumn("index", F.lit(f"$test_index-1"))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"))
      .withColumn("new", F.lit(null))
      .withColumn("a" , F.lit(-1))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 4. Read indexes with wildcards") {
    val query = new OTLQuery(
      id = 0,
      original_otl = s"""search index=* | table _time, floor, room, metric_name, value, index, _raw""",
      service_otl = s""" | otstats {"*": {"query": "", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
      tws = 0,
      twf = 0,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = true,
      preview = false
    )
    var actual = new Converter(query).run
    var df1 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors-1.csv")
      .withColumn("index", F.lit(f"$test_index-1").cast(StringType))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
        F.col("value"), F.col("_raw"), F.col("index"))
    var df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors-2.csv")
      .withColumn("index", F.lit(f"$test_index-2").cast(StringType))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
        F.col("value"), F.col("_raw"), F.col("index"))
    df1 = setNullableStateOfColumn(df1, "index", true)
    df2 = setNullableStateOfColumn(df2, "index", true)
    var expected = df1.union(df2)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }


  test("READ => TEST 5. Read indexes with wildcard ") {
    val query = new OTLQuery(
      id = 0,
      original_otl = s"""search index=${test_index}* | table _time, floor, room, metric_name, value, index, _raw""",
      service_otl = s""" | otstats {"$test_index*": {"query": "", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
      tws = 0,
      twf = 0,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = true,
      preview = false
    )
    var actual = new Converter(query).run
    var df1 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors-1.csv")
      .withColumn("index", F.lit(f"$test_index-1").cast(StringType))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
        F.col("value"), F.col("_raw"), F.col("index"))
    var df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors-2.csv")
      .withColumn("index", F.lit(f"$test_index-2").cast(StringType))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
        F.col("value"), F.col("_raw"), F.col("index"))
    df1 = setNullableStateOfColumn(df1, "index", true)
    df2 = setNullableStateOfColumn(df2, "index", true)
    var expected = df1.union(df2)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 6. Read indexes with wildcard. One of indexes is not exist ") {
    val non_existent_index = "non_existent_index"
    val query = new OTLQuery(
      id = 0,
      original_otl = s"""search index=$test_index-1 index=$non_existent_index | table _time, floor, room, metric_name, value, index, _raw""",
      service_otl = s""" | otstats {"$test_index-1": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index": {"query": "", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
      tws = 0,
      twf = 0,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = true,
      preview = false
    )
    var actual = new Converter(query).run
    var expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors-1.csv")
      .withColumn("index", F.lit(f"$test_index-1"))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
        F.col("value"), F.col("_raw"), F.col("index"))
    expected = setNullableStateOfColumn(expected, "index", true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 7. Read indexes with wildcard. All indexes is not exist ") {
    val non_existent_index = "non_existent_index"
    val query = new OTLQuery(
      id = 0,
      original_otl = s"""search index=$non_existent_index-1 index=$non_existent_index-2 | table _time, floor, room, metric_name, value, index, _raw""",
      service_otl = s""" | otstats {"$non_existent_index-1": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index-2": {"query": "", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
      tws = 0,
      twf = 0,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = true,
      preview = false
    )

    val thrown = intercept[Exception] {
      var actual = new Converter(query).run
    }
    assert(thrown.getMessage.endsWith(s"Index not found: $non_existent_index-2"))
  }

  test("READ => TEST 8.Read index with filter on existing field") {
    val query = new OTLQuery(
      id = 0,
      original_otl = s"""search index=$test_index-1 metric_name!="temperature_celsius" AnD NOT value>15.0 oR value>25.0  | table _time, floor, room, metric_name, value, index, _raw""",
      service_otl = s""" | otstats {"$test_index-1": {"query": "!(metric_name=\\\"temperature_celsius\\\") AnD !(value>15.0) oR value>25.0", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
      tws = 0,
      twf = 0,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = true,
      preview = false
    )
    var actual = new Converter(query).run
    var expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors-1.csv")
      .withColumn("index", F.lit(f"$test_index-1"))
      .withColumn("_time", col("_time").cast(LongType))
      .filter((!(F.col("metric_name") === "temperature_celsius") && (F.col("value") <= 15.0)) || F.col("value") > 25.0)
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
        F.col("value"), F.col("_raw"), F.col("index"))
    expected = setNullableStateOfColumn(expected, "index", true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 9. Read fields with wildcards") {
    val otlQuery = createQuery(""" fields _time, metric*, *iption, *oo*  """)
    var actual = new Converter(otlQuery).run
    var expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors-1.csv")
      .withColumn("index", F.lit(f"$test_index-1"))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("description"),
        F.col("metric_name"), F.col("metric_long_name"))
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 10.Read index with filter on existing field and space") {
    val query = new OTLQuery(
      id = 0,
      original_otl = s"""search index=$test_index-1 met.ric_name="temperature_celsius" value>15.0 | table _time, floor, room, metric_name, value, index, _raw""",
      service_otl = s""" | otstats {"$test_index-1": {"query": "met.ric_name=\\\"temperature_celsius\\\" AND value>15.0 AND metric_long_name=\\\"test_metric\\\"", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
      tws = 0,
      twf = 0,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = true,
      preview = false
    )
    var actual = new Converter(query).run
    var expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors-1.csv")
      .withColumn("index", F.lit(f"$test_index-1"))
      .withColumn("_time", col("_time").cast(LongType))
      .filter((F.col("metric_name") === "temperature_celsius") && (F.col("value") > 15.0))
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
        F.col("value"), F.col("_raw"), F.col("index"))
    expected = setNullableStateOfColumn(expected, "index", true)
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

}