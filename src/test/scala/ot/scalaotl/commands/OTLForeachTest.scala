package ot.scalaotl.commands

class OTLForeachTest extends CommandTest {

  /**
+----------+----+---+----------+----------+-----------+-----------+-----+
|_time     |text|num|num{}.1val|num{}.2val|text.1val  |text.2val  |_raw |
+----------+----+---+----------+----------+-----------+-----------+-----+
|1570007900|RUB |1  |10        |100       |RUB.0      |RUB.00     | ... |
|1570008000|USD |2  |20        |200       |USD.0      |USD.00     | ... |
|1570008100|EUR |3  |30        |300       |EUR.0      |EUR.00     | ... |
|1570008200|GPB |4  |40        |400       |GPB.0      |GPB.00     | ... |
|1570008300|DRM |5  |50        |500       |DRM.0      |DRM.00     | ... |
+----------+----+---+----------+----------+-----------+-----------+-----+
  */

  override val dataset: String = """[ 
    {"_time":1570007900,"text":"RUB","num":1,"num[].1val":10,"num[].2val":100,"text.1val":"RUB.0","text.2val":"RUB.00","_raw":"_time=1570007900 text=RUB num=1 num{}.1val=10 num{}.2val=100 text.1val=RUB.0 text.2val=RUB.00 "},
    {"_time":1570008000,"text":"USD","num":2,"num[].1val":20,"num[].2val":200,"text.1val":"USD.0","text.2val":"USD.00","_raw":"_time=1570008000 text=USD num=2 num{}.1val=20 num{}.2val=200 text.1val=USD.0 text.2val=USD.00 "},
    {"_time":1570008100,"text":"EUR","num":3,"num[].1val":30,"num[].2val":300,"text.1val":"EUR.0","text.2val":"EUR.00","_raw":"_time=1570008100 text=EUR num=3 num{}.1val=30 num{}.2val=300 text.1val=EUR.0 text.2val=EUR.00 "},
    {"_time":1570008200,"text":"GPB","num":4,"num[].1val":40,"num[].2val":400,"text.1val":"GPB.0","text.2val":"GPB.00","_raw":"_time=1570008200 text=GPB num=4 num{}.1val=40 num{}.2val=400 text.1val=GPB.0 text.2val=GPB.00 "},
    {"_time":1570008300,"text":"DRM","num":5,"num[].1val":50,"num[].2val":500,"text.1val":"DRM.0","text.2val":"DRM.00","_raw":"_time=1570008300 text=DRM num=5 num{}.1val=50 num{}.2val=500 text.1val=DRM.0 text.2val=DRM.00 "}
  ]"""

  test("Unit test 0. Command: | foreach") {
    val actual = execute(
      """eval total=5, num=tonumber(num)
      | foreach num, text [eval total = total + '<<FIELD>>']
      | fields total """,
      jsonToDf(dataset)
    )
    val expected = """[
                     |{"total":"6.0RUB"},
                     |{"total":"7.0USD"},
                     |{"total":"8.0EUR"},
                     |{"total":"9.0GPB"},
                     |{"total":"10.0DRM"}
                     |]""".stripMargin

    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
  }

  test("Unit test 1. Command: | foreach") {
    val actual = execute(
      """eval total=""
      | foreach te*val [eval total = total + '<<FIELD>>' + "<<MATCHSTR>>" + "__"]
      | fields total """,
      jsonToDf(dataset)
    )
    val expected = """[
                     |{"total":"RUB.0xt.1__RUB.00xt.2__"},
                     |{"total":"USD.0xt.1__USD.00xt.2__"},
                     |{"total":"EUR.0xt.1__EUR.00xt.2__"},
                     |{"total":"GPB.0xt.1__GPB.00xt.2__"},
                     |{"total":"DRM.0xt.1__DRM.00xt.2__"}
                     |]""".stripMargin

    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
  }

  test("Unit test 2. Command: | foreach") {
    val actual = execute(
      """eval total=""
      | foreach te*, num [eval total = total + '<<FIELD>>' ]
      | fields total """,
      jsonToDf(dataset)
    )
    val expected = """[
      {"total":"RUB.0RUB.00RUB1"},
      {"total":"USD.0USD.00USD2"},
      {"total":"EUR.0EUR.00EUR3"},
      {"total":"GPB.0GPB.00GPB4"},
      {"total":"DRM.0DRM.00DRM5"}
    |]""".stripMargin

    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
  }

  test("Fields test 0. Command: foreach") {
    val actual = getFieldsUsed(
      """ foreach te*, text{}.empty, num, _time [eval total = total + '<<FIELD>>'] """
    )
    // Must be sorted lexicographically, separated with ', ' (comma + space)
    // Ex.: """ _raw, _time """
    val expected = """ _time, num, te*, text{}.empty """.trim
    assert(actual == expected, f"\nResult : $actual\n---\nExpected : $expected")
  }

  test("Integration test 0. Command: | foreach") {
    val actual = execute(
      """eval total=""
      | foreach te*val [eval total = total + '<<FIELD>>' + "<<MATCHSTR>>" + "__"]
      | fields total """
    )
    val expected = """[
                     |{"total":"RUB.0xt.1__RUB.00xt.2__"},
                     |{"total":"USD.0xt.1__USD.00xt.2__"},
                     |{"total":"EUR.0xt.1__EUR.00xt.2__"},
                     |{"total":"GPB.0xt.1__GPB.00xt.2__"},
                     |{"total":"DRM.0xt.1__DRM.00xt.2__"}
                     |]""".stripMargin

    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected\n")
  }
  
}
