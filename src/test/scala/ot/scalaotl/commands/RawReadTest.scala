package ot.scalaotl.commands

import java.io.File

import ot.dispatcher.OTLQuery

import scala.reflect.io.Directory

class RawReadTest extends CommandTest {

  /**
   * +----------+----+---+----------+----------+-----------+-----------+-----+
   * |_time     |text|num|num{}.1val|num{}.2val|text{}.1val|text{}.2val|_raw |
   * +----------+----+---+----------+----------+-----------+-----------+-----+
   * |1570007900|RUB |1  |10        |100       |RUB.0      |RUB.00     | ... |
   * |1570008000|USD |2  |20        |200       |USD.0      |USD.00     | ... |
   * |1570008100|EUR |3  |30        |300       |EUR.0      |EUR.00     | ... |
   * |1570008200|GPB |4  |40        |400       |GPB.0      |GPB.00     | ... |
   * |1570008300|DRM |5  |50        |500       |DRM.0      |DRM.00     | ... |
   * +----------+----+---+----------+----------+-----------+-----------+-----+
   */

  override val dataset: String =
    """[
    {"_time":1570007900,"_raw":"_time=1570007900 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 "},
    {"_time":1570008000,"_raw":"_time=1570008000 text=USD num=2 num{1}.val=20 num{2}.val=200 text{1}.val=USD.0 text{2}.val=USD.00 "},
    {"_time":1570008100,"_raw":"_time=1570008100 text=EUR num=3 num{1}.val=30 num{2}.val=300 text{1}.val=EUR.0 text{2}.val=EUR.00 "},
    {"_time":1570008200,"_raw":"_time=1570008200 text=GPB num=4 num{1}.val=40 num{2}.val=400 text{1}.val=GPB.0 text{2}.val=GPB.00 "},
    {"_time":1570008300,"_raw":"_time=1570008300 text=DRM num=5 num{1}.val=50 num{2}.val=500 text{1}.val=DRM.0 text{2}.val=DRM.00 "}
  ]"""

  override def createQuery(command_otl: String, tws: Int = 0, twf: Int = 0): OTLQuery ={
    val otlQuery = new OTLQuery(
      id = 0,
      original_otl = s"search index=$test_index | $command_otl",
      service_otl = s""" | read {"$test_index": {"query": "", "tws": "$tws", "twf": "$twf"}} |  $command_otl """,
      tws = tws,
      twf = twf,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = false,
      preview = false
    )
    log.debug(s"otlQuery: $otlQuery.")
    otlQuery
  }

  test("READ => TEST 0. Simple correct read _time and _raw") {
    val actual = execute(
      """ table _time, _raw  """
    )
    val expected =
      """[
      {"_time":1570007900,"_raw":"_time=1570007900 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 "},
      {"_time":1570008000,"_raw":"_time=1570008000 text=USD num=2 num{1}.val=20 num{2}.val=200 text{1}.val=USD.0 text{2}.val=USD.00 "},
      {"_time":1570008100,"_raw":"_time=1570008100 text=EUR num=3 num{1}.val=30 num{2}.val=300 text{1}.val=EUR.0 text{2}.val=EUR.00 "},
      {"_time":1570008200,"_raw":"_time=1570008200 text=GPB num=4 num{1}.val=40 num{2}.val=400 text{1}.val=GPB.0 text{2}.val=GPB.00 "},
      {"_time":1570008300,"_raw":"_time=1570008300 text=DRM num=5 num{1}.val=50 num{2}.val=500 text{1}.val=DRM.0 text{2}.val=DRM.00 "}
    ]"""
    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
  }

  test("READ => TEST 1. Add empty cols") {
    val actual = execute(
      """ eval a = if (new == 1, 1, -1) | table _time, new, a"""
    )
    val expected =
      """[
      {"_time":1570007900,"a":-1,"new":"null"},
      {"_time":1570008000,"a":-1,"new":"null"},
      {"_time":1570008100,"a":-1,"new":"null"},
      {"_time":1570008200,"a":-1,"new":"null"},
      {"_time":1570008300,"a":-1,"new":"null"}
    ]"""
    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
  }


  test("READ => TEST 2.1. Create multi-value cols") {
    val actual = execute("""table 'num{}.val', 'text{}.val' | eval res = mvzip('num{}.val', 'text{}.val') """)// table res, num*, text*
    val expected =
      """[
        {"num{}.val":["10","100"],"text{}.val":["RUB.0","RUB.00"],"res":["10 RUB.0","100 RUB.00"]},
        {"num{}.val":["20","200"],"text{}.val":["USD.0","USD.00"],"res":["20 USD.0","200 USD.00"]},
        {"num{}.val":["30","300"],"text{}.val":["EUR.0","EUR.00"],"res":["30 EUR.0","300 EUR.00"]},
        {"num{}.val":["40","400"],"text{}.val":["GPB.0","GPB.00"],"res":["40 GPB.0","400 GPB.00"]},
        {"num{}.val":["50","500"],"text{}.val":["DRM.0","DRM.00"],"res":["50 DRM.0","500 DRM.00"]}
        ]"""
    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
  }



//
//  test("READ => TEST 2.2. Create multi-value cols") {
//    val otherDataset: String =
//      """[
//        |{"_time":1568026476854,"_raw":"{\"listField\": [0,10], \"nestedField\": [{\"val\": 1}, {\"val\": 2}]}"}
//        |]""".stripMargin
//    val df = jsonToDf(otherDataset)
//    val backetPath = f"$tmpDir/indexes/${test_index}2.2/bucket-0-${Int.MaxValue}-${System.currentTimeMillis / 1000}"
//    df.write.parquet(backetPath)
//    if (externalSchema)
//      new java.io.PrintWriter(backetPath + "/all.schema") {
//        write(df.schema.toDDL.replace(",", "\n"));
//        close()
//      }
//    val query = new OTLQuery(
//      id = 0,
//      original_otl = s"search index=${test_index}2.2| table listField{}, nestedField{}.val",
//      service_otl = "| read {\"" + test_index + "2.2\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}} | table listField{}, nestedField{}.val ",
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
  test("READ => TEST 3. Replace square brackets with curly brackets in col names") {
    val actual = execute(""" fields 'num{1}.val', 'text{2}.val' """)
    val expected =
      """[
        |{"num{1}.val":"10","text{2}.val":"RUB.00"},
        |{"num{1}.val":"20","text{2}.val":"USD.00"},
        |{"num{1}.val":"30","text{2}.val":"EUR.00"},
        |{"num{1}.val":"40","text{2}.val":"GPB.00"},
        |{"num{1}.val":"50","text{2}.val":"DRM.00"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
  }

  test("READ => TEST 4. Read fields with wildcards") {
    val actual = execute(""" fields num*, text* """)
    val expected =
      """[
        |{"num":"1","num{}.val":["10","100"],"text":"RUB","text{}.val":["RUB.0","RUB.00"]},
        |{"num":"2","num{}.val":["20","200"],"text":"USD","text{}.val":["USD.0","USD.00"]},
        |{"num":"3","num{}.val":["30","300"],"text":"EUR","text{}.val":["EUR.0","EUR.00"]},
        |{"num":"4","num{}.val":["40","400"],"text":"GPB","text{}.val":["GPB.0","GPB.00"]},
        |{"num":"5","num{}.val":["50","500"],"text":"DRM","text{}.val":["DRM.0","DRM.00"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
  }
//
//  test("READ => TEST 5. Apply time range") {
//    val actual = execute(""" fields _time, _raw """, 1570008000, 1570008200)
//    val expected =
//      """[
//      {"_time":1570008000,"_raw":"_time=1570008000 text=USD num=2 num{1}.val=20 num{2}.val=200 text{1}.val=USD.0 text{2}.val=USD.00 "},
//      {"_time":1570008100,"_raw":"_time=1570008100 text=EUR num=3 num{1}.val=30 num{2}.val=300 text{1}.val=EUR.0 text{2}.val=EUR.00 "}
//    ]"""
//    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
//  }
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