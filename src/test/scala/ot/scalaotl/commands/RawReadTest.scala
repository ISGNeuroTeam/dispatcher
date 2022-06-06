package ot.scalaotl.commands


import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => F}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, NullType, StringType, StructField, StructType}
import ot.dispatcher.OTLQuery
import ot.scalaotl.Converter
import ot.scalaotl.utils.searchinternals.Timerange.{duration_cache, log}

import scala.collection.mutable.ListBuffer
class RawReadTest extends CommandTest {

  val start_time = 1649145660
  val finish_time = 1663228860
//  val finish_time = 1649550000


  override def createQuery(command_otl: String, tws: Int = 0, twf: Int = 0): OTLQuery ={
    val otlQuery = new OTLQuery(
      id = 0,
      original_otl = s"search index=$test_index-0 | $command_otl",
      service_otl = s"""| read {"$test_index-0": {"query": "", "tws": "$tws", "twf": "$twf"}} |  $command_otl """,
      tws = tws,
      twf = twf,
      cache_ttl = 0,
      indexes = Array(s"$test_index-0"),
      subsearches = Map(),
      username = "admin",
      field_extraction = false,
      preview = false
    )
    log.debug(s"otlQuery: $otlQuery.")
    otlQuery
  }

  def getIndexDF(index : String): DataFrame ={
    val filenames = ListBuffer[String]()
    try {
      val status = fs_disk.listStatus(new Path(s"$tmpDir/indexes/$index"))
      status.foreach(x => filenames += s"$tmpDir/indexes/$index/${x.getPath.getName}")
    }
    catch { case e: Exception => log.debug(e);}
    spark.read.options(Map("recursiveFileLookup"->"true")).schema(stfeDfSchema)
      .parquet(filenames.seq: _*)
      .withColumn("index", F.lit(index))
  }

  test("READ => TEST 1. Simple read only _time and _raw fields") {
    val otlQuery = createQuery("""table _time, _raw""")
    val actual = new Converter(otlQuery).run
    val expected = getIndexDF(s"$test_index-0")
      .select(F.col("_time"), F.col("_raw"))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 2. Apply time range") {
    val otlQuery = createQuery("""table _time, _raw""", start_time, finish_time)
    val actual = new Converter(otlQuery).run
    val expected = getIndexDF(s"$test_index-0")
      .select(F.col("_time"), F.col("_raw"))
      .filter(F.col("_time").between(start_time, finish_time))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 3. Add empty cols") {
    val otlQuery = createQuery("""eval a = if (new == 1, 1, -1) | table _time, new, a""")
    val actual = new Converter(otlQuery).run
    val expected = getIndexDF(s"$test_index-0")
      .select(F.col("_time"))
      .withColumn("new", F.lit(null).cast(StringType))
      .withColumn("a" , F.lit(-1))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 4. Read indexes with wildcards") {
    val query = new OTLQuery(
      id = 0,
      original_otl = s"""search index=* | table _time, _raw""",
      service_otl = s"""| read {"*": {"query": "", "tws": 0, "twf": 0}} | table _time, _raw""",
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
    var expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField("_time", LongType), StructField("_raw", StringType))))
    for(i <- stfeRawSeparators.indices) {
      val df = getIndexDF(f"${test_index}-${i}").select(F.col("_time"), F.col("_raw"))
      expected = df.union(expected)
    }
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 5. Read indexes with wildcard ") {
    val query = new OTLQuery(
      id = 0,
      original_otl = s"""search index=${test_index}* | table _time, _raw""",
      service_otl = s"""| read {"$test_index*": {"query": "", "tws": 0, "twf": 0}} | table _time, _raw""",
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
    var expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField("_time", LongType), StructField("_raw", StringType))))
    for(i <- stfeRawSeparators.indices) {
      val df = getIndexDF(f"${test_index}-${i}").select(F.col("_time"), F.col("_raw"))
      expected = df.union(expected)
    }
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 6. Read indexes with wildcard. One of indexes is not exist ") {
    val non_existent_index = "non_existent_index"
    val query = new OTLQuery(
      id = 0,
      original_otl = s"""search index=$test_index-1 index=$non_existent_index | table _time, _raw""",
      service_otl = s"""| read {"$test_index-1": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index": {"query": "", "tws": 0, "twf": 0}} | table _time, _raw""",
      tws = 0,
      twf = 0,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = true,
      preview = false
    )
    val actual = new Converter(query).run
    val expected = getIndexDF(s"$test_index-1")
      .select(F.col("_time"), F.col("_raw"))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 7. Read indexes with wildcard. All indexes is not exist ") {
    val non_existent_index = "non_existent_index"
    val query = new OTLQuery(
      id = 0,
      original_otl = s"""search index=$non_existent_index-1 index=$non_existent_index-2 | table _time, _raw""",
      service_otl = s""" | read {"$non_existent_index-1": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index-2": {"query": "", "tws": 0, "twf": 0}}  | table _time, _raw""",
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
      new Converter(query).run
    }
    assert(thrown.getMessage.endsWith(s"Index not found: $non_existent_index-2"))
  }

  def stfeExtractionTest(k: Int): Unit ={
    val query = new OTLQuery(
      id = 0,
      original_otl = s"""search index=${test_index}-$k | table _time, _raw, metric_name, metric_long_name, value | sort _time, metric_name, value""",
      service_otl = s"""| read {"$test_index-$k": {"query": "", "tws": $start_time, "twf": $finish_time}} | table _time, _raw, metric_name, metric_long_name, value""",
      tws = start_time,
      twf = finish_time,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = true,
      preview = false
    )
    var actual = new Converter(query).run
    actual = actual.withColumn("value", col("value").cast(DoubleType))
    val expected = getIndexDF(s"$test_index-$k")
      .filter(F.col("_time").between(start_time, finish_time))
      .select(
        F.col("_time"), F.col("_raw"),
        F.col("metric_name"), F.col("metric_long_name"), F.col("value")
      )
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 8. Search-time field extractions from json") {
    stfeExtractionTest(0)
  }

  test("READ => TEST 9. Search-time field extractions from _raw") {
    stfeExtractionTest(1)
  }

  test("READ => TEST 10. Search-time field extractions from _raw") {
    stfeExtractionTest(2)
  }

  test("READ => TEST 11. Search-time field extractions from _raw") {
    stfeExtractionTest(3)
  }

  test("READ => TEST 12. Search-time field extractions from _raw") {
    stfeExtractionTest(4)
  }

  test("READ => TEST 13. Search-time field extractions from _raw") {
    stfeExtractionTest(5)
  }

  test("READ => TEST 14. Search-time field extractions from _raw") {
    stfeExtractionTest(6)
  }

  test("READ => TEST 15. Search-time field extractions from _raw") {
    stfeExtractionTest(7)
  }

  test("READ => TEST 16. Search-time field extractions from _raw") {
    stfeExtractionTest(8)
  }

}