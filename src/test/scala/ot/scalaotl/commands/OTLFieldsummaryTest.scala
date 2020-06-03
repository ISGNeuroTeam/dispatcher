package ot.scalaotl.commands

class OTLFieldsummaryTest extends CommandTest{

  test("Test 0. Command: | fieldsummary") {
    val actual = execute("""table serialField, random_Field | fieldsummary """)
    val expected = """[
                     |{"column":"random_Field","count":"10","max":"60","mean":"13.0","min":"-100","stddev":"63.60468186820492"},
                     |{"column":"serialField","count":"10","max":"9","mean":"4.5","min":"0","stddev":"3.0276503540974917"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  //BUG1 В команде не работает отображения summary по конкретным полям
  ignore("Test 1. Command: | fieldsummary with a specific field") {
    val actual = execute("""table serialField, random_Field | fieldsummary random_Field""")
    val expected = """[
                     |{"column":"random_Field","count":"10","max":"60","mean":"13.0","min":"-100","stddev":"63.60468186820492"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
