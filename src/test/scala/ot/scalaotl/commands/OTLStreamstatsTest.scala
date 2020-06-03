package ot.scalaotl.commands

class OTLStreamstatsTest extends CommandTest {

  override val dataset: String = """[
    {"_time":1570008000,"text":"RUB","num":1,"_raw":"_time=1570008000 text=RUB num=1 "},
    {"_time":1570008020,"text":"USD","num":2,"_raw":"_time=1570008020 text=USD num=2 "},
    {"_time":1570008040,"text":"RUB","num":3,"_raw":"_time=1570008040 text=RUB num=3 "},
    {"_time":1570008060,"text":"USD","num":4,"_raw":"_time=1570008060 text=USD num=4 "},
    {"_time":1570008080,"text":"DRM","num":5,"_raw":"_time=1570008080 text=DRM num=5 "}
  ]"""

  test("Test 1. Command: | streamstats + different agg options commands + as clause + as 'multiple word field' close ") {
    val actual = execute("""streamstats count as count max(num) as "max num" min(num) as "min num" sum(num) as "sum num"  """)
    val expected =
      """[
        |{"_time":1570008000,"_raw":"_time=1570008000 text=RUB num=1 ","num":"1","count":1,"max num":"1","min num":"1","sum num":1.0},
        |{"_time":1570008020,"_raw":"_time=1570008020 text=USD num=2 ","num":"2","count":2,"max num":"2","min num":"1","sum num":3.0},
        |{"_time":1570008040,"_raw":"_time=1570008040 text=RUB num=3 ","num":"3","count":3,"max num":"3","min num":"1","sum num":6.0},
        |{"_time":1570008060,"_raw":"_time=1570008060 text=USD num=4 ","num":"4","count":4,"max num":"4","min num":"1","sum num":10.0},
        |{"_time":1570008080,"_raw":"_time=1570008080 text=DRM num=5 ","num":"5","count":5,"max num":"5","min num":"1","sum num":15.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  // BUG Не должно быть пересортировки самих событий
  ignore("Test 2. Command: | streamstats + 'by' option ") {
    val actual = execute("""streamstats count by text  """)
    val expected =
      """[
        |{"_time":1570008000,"_raw":"_time=1570008000 text=RUB num=1 ","text":"RUB","count":1},
        |{"_time":1570008020,"_raw":"_time=1570008020 text=USD num=2 ","text":"USD","count":1},
        |{"_time":1570008040,"_raw":"_time=1570008040 text=RUB num=3 ","text":"RUB","count":2},
        |{"_time":1570008060,"_raw":"_time=1570008060 text=USD num=4 ","text":"USD","count":2},
        |{"_time":1570008080,"_raw":"_time=1570008080 text=DRM num=5 ","text":"DRM","count":1}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | streamstats + 'time_window' option ") {
    val actual = execute(""" | streamstats time_window=60 count """)
    val expected =
      """[
        |{"_time":1570008000,"_raw":"_time=1570008000 text=RUB num=1 ","count":1},
        |{"_time":1570008020,"_raw":"_time=1570008020 text=USD num=2 ","count":2},
        |{"_time":1570008040,"_raw":"_time=1570008040 text=RUB num=3 ","count":3},
        |{"_time":1570008060,"_raw":"_time=1570008060 text=USD num=4 ","count":3},
        |{"_time":1570008080,"_raw":"_time=1570008080 text=DRM num=5 ","count":3}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4. Command: | streamstats + 'window' option ") {
    val actual = execute(""" | streamstats window=3 count """)
    val expected =
      """[
        |{"_time":1570008000,"_raw":"_time=1570008000 text=RUB num=1 ","count":1},
        |{"_time":1570008020,"_raw":"_time=1570008020 text=USD num=2 ","count":2},
        |{"_time":1570008040,"_raw":"_time=1570008040 text=RUB num=3 ","count":3},
        |{"_time":1570008060,"_raw":"_time=1570008060 text=USD num=4 ","count":3},
        |{"_time":1570008080,"_raw":"_time=1570008080 text=DRM num=5 ","count":3}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}