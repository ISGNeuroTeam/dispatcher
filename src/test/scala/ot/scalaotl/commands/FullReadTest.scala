package ot.scalaotl.commands

import org.apache.hadoop.fs.Path

import java.io.File
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => F}
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, NullType, StringType, StructField, StructType}
import ot.dispatcher.OTLQuery
import ot.scalaotl.Converter
import ot.scalaotl.extensions.StringExt.BetterString

import scala.collection.mutable.ListBuffer


class FullReadTest extends CommandTest {

  val start_time = 1649145660
  val finish_time = 1663228860

  override def createQuery(command_otl: String, tws: Int = 0, twf: Int = 0, read_cmd: String = "otstats"): OTLQuery ={
    val otlQuery = new OTLQuery(
      id = 0,
      original_otl = s"search index=$test_index-0 | $command_otl",
      service_otl = s""" | $read_cmd {"$test_index-0": {"query": "", "tws": "$tws", "twf": "$twf"}} |  $command_otl """,
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
    spark.read.options(Map("recursiveFileLookup"->"true")).schema(readingDatasetSchema)
      .parquet(filenames.seq: _*)
      .withColumn("index", F.lit(index))
  }

//  test("READ => TEST 1. Simple read only _time and _raw fields") {
//    val otlQuery = createQuery(""" table _time, _raw, metric_name, value  """)
//    val actual = new Converter(otlQuery).run
//    val expected = getIndexDF(s"$test_index-0")
//      .select(F.col("_time"), F.col("_raw"), F.col("metric_name"), F.col("value"))
//    compareDataFrames(actual, expected)
//  }
//
  test("READ => TEST 2. Apply time range") {
    val otlQuery = createQuery(""" table _time, _raw, metric_name, value """, start_time, finish_time)
    val actual = new Converter(otlQuery).run
    val expected = getIndexDF(s"$test_index-0")
      .select(F.col("_time"), F.col("_raw"), F.col("metric_name"), F.col("value"))
      .filter(F.col("_time").between(start_time, finish_time))
    compareDataFrames(actual, expected)
  }
//
//  test("READ => TEST 3. Add empty cols") {
//    val otlQuery = createQuery(""" eval a = if (new == 1, 1, -1) | table _time, metric_name, value, new, a""")
//    val actual = new Converter(otlQuery).run
//    val expected = getIndexDF(s"$test_index-0")
//      .select(F.col("_time"), F.col("metric_name"), F.col("value"))
//      .withColumn("new", F.lit(null))
//      .withColumn("a" , F.lit(-1))
//    compareDataFrames(actual, expected)
//  }
//
//  test("READ => TEST 4. Read indexes with wildcards") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"""search index=* | table _time, metric_name, value""",
//      service_otl = s""" | otstats {"*": {"query": "", "tws": 0, "twf": 0}} | table _time, metric_name, value""",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(s"$test_index-0"),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = true,
//      preview = false
//    )
//    var actual = new Converter(query).run
//    var expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
//      StructType(Seq(StructField("_time", LongType), StructField("metric_name", StringType),
//        StructField("value", DoubleType))))
//    for(i <- stfeRawSeparators.indices) {
//      val df = getIndexDF(f"${test_index}-${i}")
//        .select(F.col("_time"), F.col("metric_name"), F.col("value"))
//      expected = df.union(expected)
//    }
//    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
//    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
//    compareDataFrames(actual, expected)
//  }
//
//
//  test("READ => TEST 5. Read indexes with wildcard ") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"""search index=${test_index}* | table _time, metric_name, value""",
//      service_otl = s""" | otstats {"$test_index*": {"query": "", "tws": 0, "twf": 0}}  | table _time, metric_name, value""",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(s"$test_index-0"),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = true,
//      preview = false
//    )
//    var actual = new Converter(query).run
//    var expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
//      StructType(Seq(StructField("_time", LongType), StructField("metric_name", StringType),
//        StructField("value", DoubleType))))
//    for(i <- stfeRawSeparators.indices) {
//      val df = getIndexDF(f"${test_index}-${i}")
//        .select(F.col("_time"), F.col("metric_name"), F.col("value"))
//      expected = df.union(expected)
//    }
//    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
//    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
//    compareDataFrames(actual, expected)
//  }
//
//  test("READ => TEST 6. Read indexes with wildcard. One of indexes is not exist ") {
//    val non_existent_index = "non_existent_index"
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"""search index=$test_index-0 index=$non_existent_index  table _time, metric_name, value""",
//      service_otl = s""" | otstats {"$test_index-0": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index": {"query": "", "tws": 0, "twf": 0}} | table _time, metric_name, value""",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(s"$test_index-0"),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = true,
//      preview = false
//    )
//    var actual = new Converter(query).run
//    var expected = getIndexDF(s"$test_index-0")
//      .select(F.col("_time"), F.col("metric_name"), F.col("value"))
//    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
//    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
//    compareDataFrames(actual, expected)
//  }
//
//  test("READ => TEST 7. Read indexes with wildcard. All indexes is not exist ") {
//    val non_existent_index = "non_existent_index"
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"""search index=$non_existent_index-1 index=$non_existent_index-2 | table _time, floor, room, metric_name, value, index, _raw""",
//      service_otl = s""" | otstats {"$non_existent_index-1": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index-2": {"query": "", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(s"$test_index-0"),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = true,
//      preview = false
//    )
//
//    val thrown = intercept[Exception] {
//      val actual = new Converter(query).run
//    }
//    assert(thrown.getMessage.endsWith(s"Index not found: $non_existent_index-2"))
//  }
//
//  test("READ => TEST 8.Read index with filter on existing field") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"""search index=$test_index-0 metric_name!="temperature_celsius" AnD NOT value>15.0 oR value>25.0  | table _time, floor, room, metric_name, value, index, _raw""",
//      service_otl = s""" | otstats {"$test_index-0": {"query": "!(metric_name=\\\"temperature_celsius\\\") AnD !(value>15.0) oR value>25.0", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(s"$test_index-0"),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = true,
//      preview = false
//    )
//    var actual = new Converter(query).run
//    var expected = getIndexDF(s"$test_index-0")
//      .filter((!(F.col("metric_name") === "temperature_celsius") && (F.col("value") <= 15.0)) || F.col("value") > 25.0)
//      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
//        F.col("value"), F.col("_raw"), F.col("index"))
//    expected = setNullableStateOfColumn(expected, "index", true)
//    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
//    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
//    compareDataFrames(actual, expected)
//  }
//
//  test("READ => TEST 9. Read fields with wildcards") {
//    val otlQuery = createQuery(""" table _time, metric*, *iption, *oo*  """)
//    var actual = new Converter(otlQuery).run
//    var expected = getIndexDF(s"$test_index-0")
//      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("description"),
//        F.col("metric_name"), F.col("metric_long_name"))
//    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
//    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
//    compareDataFrames(actual, expected)
//  }

  test("READ => TEST 10. Create multi-value cols") {
    val otlQuery = createQuery("""fields _time, 'num{}.val', 'text{}.val' | eval res = mvzip('num{}.val', 'text{}.val') | table _time, res, num*, text*""")
    val actual = new Converter(otlQuery).run
    var expected = getIndexDF(s"$test_index-0").select(F.col("_time"),
        F.col("`text`"), F.col("`text[1].val`"), F.col("`text[2].val`"),
        F.col("`num`"), F.col("`num[1].val`"), F.col("`num[2].val`")
      )
      .withColumn("res", array(
        F.concat(F.col("`num[1].val`"), F.lit(' '), F.col("`text[1].val`")),
        F.concat(F.col("`num[2].val`"), F.lit(' '), F.col("`text[2].val`"))
      ))
      .withColumn("num{}.val", array(F.col("`num[1].val`"), F.col("`num[2].val`")))
      .withColumn("text{}.val", array(F.col("`text[1].val`"), F.col("`text[2].val`")))
      .select(F.col("_time"),  F.col("res"), F.col("`num{}.val`"), F.col("`text{}.val`"))
    expected = setNullableStateOfColumn(expected, List("res", "`num{}.val`", "`text{}.val`"), true)
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 11. Replace square brackets with curly brackets in col names") {
    val otlQuery = createQuery("""fields _time, 'num{1}.val', 'text{2}.val'""")
    val actual = new Converter(otlQuery).run
    val expected = getIndexDF(s"$test_index-0")
      .withColumnRenamed("num[1].val", "num{1}.val")
      .withColumnRenamed("text[2].val", "text{2}.val")
      .select(F.col("_time"), F.col("`num{1}.val`"), F.col("`text{2}.val`"))
    compareDataFrames(actual, expected)
  }


}