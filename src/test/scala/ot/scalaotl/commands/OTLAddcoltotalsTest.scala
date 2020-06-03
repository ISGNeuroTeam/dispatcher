package ot.scalaotl.commands

class OTLAddcoltotalsTest extends CommandTest{

  test("Test 0. Command: | addcoltotals") {
    val actual = execute("""eval random_Field=int(random_Field) | chart count by random_Field | addcoltotals """)
    val expected = """[
                     |{"random_Field":20,"count":1},
                     |{"random_Field":100,"count":1},
                     |{"random_Field":-90,"count":1},
                     |{"random_Field":10,"count":1},
                     |{"random_Field":50,"count":2},
                     |{"random_Field":60,"count":1},
                     |{"random_Field":-100,"count":1},
                     |{"random_Field":30,"count":1},
                     |{"random_Field":0,"count":1},
                     |{"random_Field":80,"count":10}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  // BUG1. Если в качестве параметра комманды addcoltotals указать имя поля, запрос падает с ошибкой
  ignore("Test 1. Command: | addcoltotals calculates the sum only for the fields in the list") {
    val actual = execute("""eval random_Field=int(random_Field) | chart count by random_Field | addcoltotals random_Field """)
    val expected = """[
                     |{"random_Field":20,"count":1},
                     |{"random_Field":100,"count":1},
                     |{"random_Field":-90,"count":1},
                     |{"random_Field":10,"count":1},
                     |{"random_Field":50,"count":2},
                     |{"random_Field":60,"count":1},
                     |{"random_Field":-100,"count":1},
                     |{"random_Field":30,"count":1},
                     |{"random_Field":0,"count":1},
                     |{"random_Field":80}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | addcoltotals. Specify a field name to add to the result set - labelfield") {
    val actual = execute("""eval random_Field=int(random_Field) | chart count by random_Field | addcoltotals labelfield=TotalResult""")
    val expected = """[
                     |{"random_Field":20,"count":1},
                     |{"random_Field":100,"count":1},
                     |{"random_Field":-90,"count":1},
                     |{"random_Field":10,"count":1},
                     |{"random_Field":50,"count":2},
                     |{"random_Field":60,"count":1},
                     |{"random_Field":-100,"count":1},
                     |{"random_Field":30,"count":1},
                     |{"random_Field":0,"count":1},
                     |{"random_Field":80,"count":10,"TotalResult":"Total"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | addcoltotals. add a label in the summary event - label") {
    val actual = execute("""eval random_Field=int(random_Field) | chart count by random_Field | addcoltotals labelfield=TotalResult label=All""")
    val expected = """[
                     |{"random_Field":20,"count":1},
                     |{"random_Field":100,"count":1},
                     |{"random_Field":-90,"count":1},
                     |{"random_Field":10,"count":1},
                     |{"random_Field":50,"count":2},
                     |{"random_Field":60,"count":1},
                     |{"random_Field":-100,"count":1},
                     |{"random_Field":30,"count":1},
                     |{"random_Field":0,"count":1},
                     |{"random_Field":80,"count":10,"TotalResult":"All"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
