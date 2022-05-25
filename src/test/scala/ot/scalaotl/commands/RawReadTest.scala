package ot.scalaotl.commands

import java.io.File
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types.{ArrayType, LongType, NullType, StringType, StructField, StructType}
import ot.dispatcher.OTLQuery
import ot.scalaotl.Converter

import scala.reflect.io.Directory

class RawReadTest extends CommandTest {



  test("READ => TEST 0. Simple correct read _time and _raw") {
    val otlQuery = createQuery(""" table _time, _raw  """)
    val actual = new Converter(otlQuery).run
    val expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors.csv")
      .withColumn("index", F.lit(f"$test_index"))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"), F.col("_raw"))
    compareDataFrames(actual, expected)
  }

  test("READ => TEST 1. Add empty cols") {
    val otlQuery = createQuery(""" eval a = if (new == 1, 1, -1) | table _time, new, a""")
    val actual = new Converter(otlQuery).run

    val expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors.csv")
      .withColumn("index", F.lit(f"$test_index"))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"))
      .withColumn("new", F.lit(null))
      .withColumn("a" , F.lit(-1))
    compareDataFrames(actual, expected)

  }

  test("READ => TEST 5. Apply time range") {
    val start_time = 1649145660
    val finish_time = 1663228860
    val otlQuery = createQuery(""" fields _time, _raw """, start_time, finish_time)
    val actual = new Converter(otlQuery).run

    val expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors.csv")
      .withColumn("index", F.lit(f"$test_index"))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"), F.col("_raw"))
      .filter(F.col("_time").between(start_time, finish_time))
    compareDataFrames(actual, expected)
  }


  test("READ => TEST 4. Read fields with wildcards") {
    val otlQuery = createQuery(""" fields _time, metric*, *iption, *oo*  """)
    val actual = new Converter(otlQuery).run
    val expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(f"$dataDir/sensors.csv")
      .withColumn("index", F.lit(f"$test_index"))
      .withColumn("_time", col("_time").cast(LongType))
      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("description"),
        F.col("metric_name"), F.col("metric_long_name"))
    compareDataFrames(actual, expected)
  }



    //  test("READ => TEST 6.Read indexes with wildcards") {
    //    val query = new OTLQuery(
    //      id = 0,
    //      original_otl = s"""search index=* | table _time, floor, room, metric_name, value, index, _raw""",
    //      service_otl = s""" | otstats {"*": {"query": "", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
    //      tws = 0,
    //      twf = 0,
    //      cache_ttl = 0,
    //      indexes = Array(test_index),
    //      subsearches = Map(),
    //      username = "admin",
    //      field_extraction = true,
    //      preview = false
    //    )
    //    var actual = new Converter(query).run
    //    var df1 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    //      .csv(f"$dataDir/sensors-1.csv")
    //      .withColumn("index", F.lit(f"$test_index-1").cast(StringType))
    //      .withColumn("_time", col("_time").cast(LongType))
    //      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
    //        F.col("value"), F.col("_raw"), F.col("index"))
    //    var df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    //      .csv(f"$dataDir/sensors-2.csv")
    //      .withColumn("index", F.lit(f"$test_index-2").cast(StringType))
    //      .withColumn("_time", col("_time").cast(LongType))
    //      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
    //        F.col("value"), F.col("_raw"), F.col("index"))
    //    df1 = setNullableStateOfColumn(df1, "index", true)
    //    df2 = setNullableStateOfColumn(df2, "index", true)
    //    var expected = df1.union(df2)
    //    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    //    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    //    compareDataFrames(actual, expected)
    //  }
    //
    //
    //  test("READ => TEST 11.Read indexes with wildcard ") {
    //    val query = new OTLQuery(
    //      id = 0,
    //      original_otl = s"""search index=${test_index}* | table _time, floor, room, metric_name, value, index, _raw""",
    //      service_otl = s""" | otstats {"$test_index*": {"query": "", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
    //      tws = 0,
    //      twf = 0,
    //      cache_ttl = 0,
    //      indexes = Array(test_index),
    //      subsearches = Map(),
    //      username = "admin",
    //      field_extraction = true,
    //      preview = false
    //    )
    //    var actual = new Converter(query).run
    //    var df1 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    //      .csv(f"$dataDir/sensors-1.csv")
    //      .withColumn("index", F.lit(f"$test_index-1").cast(StringType))
    //      .withColumn("_time", col("_time").cast(LongType))
    //      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
    //        F.col("value"), F.col("_raw"), F.col("index"))
    //    var df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    //      .csv(f"$dataDir/sensors-2.csv")
    //      .withColumn("index", F.lit(f"$test_index-2").cast(StringType))
    //      .withColumn("_time", col("_time").cast(LongType))
    //      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
    //        F.col("value"), F.col("_raw"), F.col("index"))
    //    df1 = setNullableStateOfColumn(df1, "index", true)
    //    df2 = setNullableStateOfColumn(df2, "index", true)
    //    var expected = df1.union(df2)
    //    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    //    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    //    compareDataFrames(actual, expected)
    //  }
    //
    //  test("READ => TEST 12.Read indexes with wildcard. One of indexes is not exist ") {
    //    val non_existent_index = "non_existent_index"
    //    val query = new OTLQuery(
    //      id = 0,
    //      original_otl = s"""search index=$test_index-1 index=$non_existent_index | table _time, floor, room, metric_name, value, index, _raw""",
    //      service_otl = s""" | otstats {"$test_index-1": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index": {"query": "", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
    //      tws = 0,
    //      twf = 0,
    //      cache_ttl = 0,
    //      indexes = Array(test_index),
    //      subsearches = Map(),
    //      username = "admin",
    //      field_extraction = true,
    //      preview = false
    //    )
    //    var actual = new Converter(query).run
    //    var expected = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    //      .csv(f"$dataDir/sensors-1.csv")
    //      .withColumn("index", F.lit(f"$test_index-1"))
    //      .withColumn("_time", col("_time").cast(LongType))
    //      .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
    //        F.col("value"), F.col("_raw"), F.col("index"))
    //    expected = setNullableStateOfColumn(expected, "index", true)
    //    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    //    expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
    //    compareDataFrames(actual, expected)
    //  }

    //  test("READ => TEST 13.Read indexes with wildcard. All indexes is not exist ") {
    //    val non_existent_index = "non_existent_index"
    //    val query = new OTLQuery(
    //      id = 0,
    //      original_otl = s"""search index=$non_existent_index-1 index=$non_existent_index-2 | table _time, floor, room, metric_name, value, index, _raw""",
    //      service_otl = s""" | otstats {"$non_existent_index-1": {"query": "", "tws": 0, "twf": 0}, "$non_existent_index-2": {"query": "", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
    //      tws = 0,
    //      twf = 0,
    //      cache_ttl = 0,
    //      indexes = Array(test_index),
    //      subsearches = Map(),
    //      username = "admin",
    //      field_extraction = true,
    //      preview = false
    //    )
    //
    //    val thrown = intercept[Exception] {
    //      var actual = new Converter(query).run
    //    }
    //    assert(thrown.getMessage.endsWith(s"Index not found: $non_existent_index-2"))
    //  }


    test("READ => TEST 7.Read index with filter on existing field") {
      val query = new OTLQuery(
        id = 0,
        original_otl = s"""search index=$test_index-1 metric_name=temperature_celsius AND value>20.0 | table _time, floor, room, metric_name, value, index, _raw""",
        service_otl = s""" | read {"$test_index-1": {"query": "metric_name=temperature_celsius AND value>20.0", "tws": 0, "twf": 0}}  | table _time, floor, room, metric_name, value, index, _raw""",
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
        .filter(F.col("metric_name") === "temperature_celsius")
        .filter(F.col("value") > 20.0)
        .select(F.col("_time"), F.col("floor"), F.col("room"), F.col("metric_name"),
          F.col("value"), F.col("_raw"), F.col("index"))
      expected = setNullableStateOfColumn(expected, "index", true)
      actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
      expected = expected.select(expected.columns.sorted.toSeq.map(c => col(c)):_*)
      compareDataFrames(actual, expected)
    }













//  test("READ => TEST 2.1. Create multi-value cols") {
//    val actual = execute("""table 'num{}.val', 'text{}.val' | eval res = mvzip('num{}.val', 'text{}.val') """)// table res, num*, text*
//    val expected =
//      """[
//        {"num{}.val":["10","100"],"text{}.val":["RUB.0","RUB.00"],"res":["10 RUB.0","100 RUB.00"]},
//        {"num{}.val":["20","200"],"text{}.val":["USD.0","USD.00"],"res":["20 USD.0","200 USD.00"]},
//        {"num{}.val":["30","300"],"text{}.val":["EUR.0","EUR.00"],"res":["30 EUR.0","300 EUR.00"]},
//        {"num{}.val":["40","400"],"text{}.val":["GPB.0","GPB.00"],"res":["40 GPB.0","400 GPB.00"]},
//        {"num{}.val":["50","500"],"text{}.val":["DRM.0","DRM.00"],"res":["50 DRM.0","500 DRM.00"]}
//        ]"""
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }




//
//
//  test("READ => TEST 2.2. Create multi-value cols") {
//    val otherDataset: String =
//      """[
//        |{"_time":1568026476854,"_raw":"{\"listField\": [0,10], \"nestedField\": [{\"val\": 1}, {\"val\": 2}]}"}
//        |]""".stripMargin
//    val df = jsonToDf(otherDataset)
//    val backetPath = f"$tmpDir/indexes/${test_index}2.2/bucket-0-${Int.MaxValue}-${System.currentTimeMillis / 1000}"
//    df.write.parquet(backetPath)
//
//    val backetPath2 = f"$tmpDir/indexes/cat_dog/bucket-0-${Int.MaxValue}-${System.currentTimeMillis / 1000}"
//    df.write.parquet(backetPath2)
//
//    val backetPath3 = f"$tmpDir/indexes/dogsitter/bucket-0-${Int.MaxValue}-${System.currentTimeMillis / 1000}"
//    df.write.parquet(backetPath3)
//
//    if (externalSchema)
//      new java.io.PrintWriter(backetPath + "/all.schema") {
//        write(df.schema.toDDL.replace(",", "\n"));
//        close()
//      }
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"search index=${test_index}2.2| table listField{}, nestedField{}.val",
//      service_otl = "| read {\"" + test_index + "2.2\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}," +
//        "\"test\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}" +
//        "\"*data*\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}" +
//        "} | table listField{}, nestedField{}.val ",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(test_index),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = false,
//      preview = false
//    )
//    val actual = execute(query)
//
//    val indexDir = new Directory(new File(f"$tmpDir/indexes/${test_index}2.2"))
//    indexDir.deleteRecursively()
//
//    val expected =
//      """[
//        |{"listField{}":["0","10"],"nestedField{}.val":["1","2"]}
//        |]""".stripMargin
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
//
//
//
//
//  test("READ => TEST 3. Replace square brackets with curly brackets in col names") {
//    val actual = execute(""" fields 'num{1}.val', 'text{2}.val' """)
//    val expected =
//      """[
//        |{"num{1}.val":"10","text{2}.val":"RUB.00"},
//        |{"num{1}.val":"20","text{2}.val":"USD.00"},
//        |{"num{1}.val":"30","text{2}.val":"EUR.00"},
//        |{"num{1}.val":"40","text{2}.val":"GPB.00"},
//        |{"num{1}.val":"50","text{2}.val":"DRM.00"}
//        |]""".stripMargin
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
//
//
//
//
//
//
//
//  test("READ => TEST 6.Read indexes with wildcards") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = "search index=* ",
//      service_otl = "| read {\"" + test_index.substring(0, test_index.length - 2) + "*\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}} ",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(test_index),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = false,
//      preview = false
//    )
//    val actual = execute(query)
//    val expected =
//      """[
//        |{"_raw":"_time=1570007900 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 ","_time":1570007900},
//        |{"_raw":"_time=1570008000 text=USD num=2 num{1}.val=20 num{2}.val=200 text{1}.val=USD.0 text{2}.val=USD.00 ","_time":1570008000},
//        |{"_raw":"_time=1570008100 text=EUR num=3 num{1}.val=30 num{2}.val=300 text{1}.val=EUR.0 text{2}.val=EUR.00 ","_time":1570008100},
//        |{"_raw":"_time=1570008200 text=GPB num=4 num{1}.val=40 num{2}.val=400 text{1}.val=GPB.0 text{2}.val=GPB.00 ","_time":1570008200},
//        |{"_raw":"_time=1570008300 text=DRM num=5 num{1}.val=50 num{2}.val=500 text{1}.val=DRM.0 text{2}.val=DRM.00 ","_time":1570008300}
//        |]""".stripMargin
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
//
//  test("READ => TEST 7.Read index with filter on existing field") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"search index=${test_index} num{1}.val=10",
//      service_otl = "| read {\"" + test_index + "\": {\"query\": \"('num{1}.val'=10)\", \"tws\": 0, \"twf\": 0}} ",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(test_index),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = true,
//      preview = false
//    )
//    val actual = execute(query)
//    val expected =
//      """[
//        |{"_time":1570007900,"_raw":"_time=1570007900 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 ","num{1}.val":"10"}
//        |]""".stripMargin
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
//
//  test("READ => TEST 8.Read index with filter on not existing field") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"search index=${test_index} abc.val=null",
//      service_otl = "| read {\"" + test_index + "\": {\"query\": \"(abc.val=\\\"null\\\")\", \"tws\": 0, \"twf\": 0}} ",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(test_index),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = false,
//      preview = false
//    )
//    val actual = execute(query)
//    val expected =
//      """[
//        |{"_raw":"_time=1570007900 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 ","_time":1570007900},
//        |{"_raw":"_time=1570008000 text=USD num=2 num{1}.val=20 num{2}.val=200 text{1}.val=USD.0 text{2}.val=USD.00 ","_time":1570008000},
//        |{"_raw":"_time=1570008100 text=EUR num=3 num{1}.val=30 num{2}.val=300 text{1}.val=EUR.0 text{2}.val=EUR.00 ","_time":1570008100},
//        |{"_raw":"_time=1570008200 text=GPB num=4 num{1}.val=40 num{2}.val=400 text{1}.val=GPB.0 text{2}.val=GPB.00 ","_time":1570008200},
//        |{"_raw":"_time=1570008300 text=DRM num=5 num{1}.val=50 num{2}.val=500 text{1}.val=DRM.0 text{2}.val=DRM.00 ","_time":1570008300}
//        |]""".stripMargin
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
//
//  test("READ => TEST 9.1. Read index with filter on not existing field with negation") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"search index=${test_index} abc.val!=null",
//      service_otl = "| read {\"" + test_index + "\": {\"query\": \"(abc.val!=\\\"null\\\")\", \"tws\": 0, \"twf\": 0}} ", //.val
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(test_index),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = false,
//      preview = false
//    )
//    val actual = execute(query)
//    val expected = "[]"
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
//  test("READ => TEST 9.2. Read index with filter on not existing field with negation and {} ") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"search index=${test_index} text{5}.val!=null",
//      service_otl = "| read {\"" + test_index + "\": {\"query\": \"('text{5}.val'!=\\\"null\\\")\", \"tws\": 0, \"twf\": 0}} ", //.val
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(test_index),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = false,
//      preview = false
//    )
//    val actual = execute(query)
//    val expected = "[]"
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
//
//  test("READ => TEST 10.Read index with filter on not existing field with negation and {} ") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"""search index=$test_index text="RUB" text{2}.val!=null""",
//      service_otl = "| read {\"" + test_index + "\": {\"query\": \"text=\\\"RUB\\\" AND 'text{2}.val'!=\\\"null\\\"\", \"tws\": 0, \"twf\": 0}} ",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(test_index),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = false,
//      preview = false
//    )
//    val actual = execute(query)
//    val expected =
//      """[
//        |{"_raw":"_time=1570007900 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 ","_time":1570007900,"text":"RUB","text{2}.val":"RUB.00"}
//        |]""".stripMargin
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
//  test("READ => TEST 10.1 Read with filter with symbols '(' and ')' s' ") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"""search index=${test_index} (text=\"RUB\" text{2}.val!=null) OR 1=1""",
//      service_otl = "| read {\"" + test_index + "\": {\"query\": \"((text=\\\"RUB\\\") AND ('text{2}.val'!=\\\"null\\\") OR (1=1))\", \"tws\": 0, \"twf\": 0}} ",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(test_index),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = false,
//      preview = false
//    )
//    val actual = execute(query)
//    val expected =
//      """[
//        |{"_raw":"_time=1570007900 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 ","_time":1570007900,"text":"RUB","text{2}.val":"RUB.00"}
//        |]""".stripMargin
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
//  test("READ => TEST 11.Read indexes with wildcard ") {
//    val otherDataset: String =
//      """[
//        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}"},
//        |{"_time":1568026476854,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}"},
//        |{"_time":1568026476854,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}"},
//        |{"_time":1568026476854,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}"},
//        |{"_time":1568026476854,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}"},
//        |{"_time":1568026476854,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}"},
//        |{"_time":1568026476854,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}"},
//        |{"_time":1568026476854,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}"},
//        |{"_time":1568026476854,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}"},
//        |{"_time":1568026476854,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}"}
//        |]"""
//    val df = jsonToDf(otherDataset)
//    val bucketPath = f"$tmpDir/indexes/${test_index}11/bucket-0-${Int.MaxValue}-${System.currentTimeMillis / 1000}"
//    df.write.parquet(bucketPath)
//    if (externalSchema)
//      new java.io.PrintWriter(bucketPath + "/all.schema") {
//        write(df.schema.toDDL.replace(",", "\n"));
//        close()
//      }
//
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = "search index=* |dedup index",
//      service_otl = "| read {\"" + test_index + "\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}, \"" + test_index + "11\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}} |dedup index",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(test_index),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = false,
//      preview = false
//    )
//
//    val actual = execute(query)
//
//    val indexDir = new Directory(new File(f"$tmpDir/indexes/${test_index}11"))
//    indexDir.deleteRecursively()
//
//    val expected =
//      """[
//        |{"_time":1570007900,"_raw":"_time=1570007900 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 ","index":"test_index-RawReadTest"},
//        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","index":"test_index-RawReadTest11"}
//        |]""".stripMargin
//
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
//
//  test("READ => TEST 12.Read indexes with wildcard. One of indexes is not exist ") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = "search index=* | dedup index",
//      service_otl = "| read {\"" + test_index + "\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}, \"" + test_index + "12\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}} |dedup index",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(test_index),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = false,
//      preview = false
//    )
//
//    val actual = execute(query)
//    val expected =
//      """[
//        |{"_raw":"_time=1570007900 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 ","_time":1570007900,"index":"test_index-RawReadTest"}
//        |]""".stripMargin
//
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
//
//  test("READ => TEST 13.Read indexes with wildcard. All indexes is not exist ") {
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = "search index=* |dedup index",
//      service_otl = "| read {\"main\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}, \"main2\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}} |dedup index",
//      tws = 0,
//      twf = 0,
//      cache_ttl = 0,
//      indexes = Array(test_index),
//      subsearches = Map(),
//      username = "admin",
//      field_extraction = false,
//      preview = false
//    )
//
//    val thrown = intercept[Exception] {
//      execute(query)
//    }
//    assert(thrown.getMessage.endsWith("Index not found: main2"))
//  }
}