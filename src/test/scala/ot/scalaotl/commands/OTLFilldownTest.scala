package ot.scalaotl.commands

class OTLFilldownTest extends CommandTest {
  override val dataset =
    """[
       {"_time": 0, "_raw": "{\"ID\":0,\"random_Field\": \"100\",\"field\": 10}"},
       {"_time": 0, "_raw": "{\"ID\":0}"},
       {"_time": 0, "_raw": "{\"ID\":0,\"random_Field\": \"150\"}"},
       {"_time": 0, "_raw": "{\"ID\":0}"},
       {"_time": 0, "_raw": "{\"ID\":1}"},
       {"_time": 0, "_raw": "{\"ID\":1,\"random_Field\": \"830\",\"field\": 20}"},
       {"_time": 0, "_raw": "{\"ID\":1}"},
       {"_time": 0, "_raw": "{\"ID\":1}"}
       ]"""

  test("Test 0. Filldown without directly fields enumeration and without defining of them (filldown by all columns). Command: table a | filldown") {
    val actual = execute("""filldown | table field, random_Field, ID"""")
    val expected = """[
    {"field":"10","random_Field":"100","ID":"0"},
    {"field":"10","random_Field":"100","ID":"0"},
    {"field":"10","random_Field":"150","ID":"0"},
    {"field":"10","random_Field":"150","ID":"0"},
    {"field":"10","random_Field":"150","ID":"1"},
    {"field":"20","random_Field":"830","ID":"1"},
    {"field":"20","random_Field":"830","ID":"1"},
    {"field":"20","random_Field":"830","ID":"1"}
    ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Filldown without directly fields enumeration and with defining of them (filldown by null containing columns). Command: table a | filldown defineTargetColumns=true") {
    val actual = execute("""| filldown defineTargetColumns=true | table field, random_Field, ID""")
    val expected =
      """[
    {"field":"10","random_Field":"100","ID":"0"},
    {"field":"10","random_Field":"100","ID":"0"},
    {"field":"10","random_Field":"150","ID":"0"},
    {"field":"10","random_Field":"150","ID":"0"},
    {"field":"10","random_Field":"150","ID":"1"},
    {"field":"20","random_Field":"830","ID":"1"},
    {"field":"20","random_Field":"830","ID":"1"},
    {"field":"20","random_Field":"830","ID":"1"}
    ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: table a | filldown field") {
    val actual = execute("""| filldown field | table field """)
    val expected = """[
        {"field" : "10"},
        {"field" : "10"},
        {"field" : "10"},
        {"field" : "10"},
        {"field" : "10"},
        {"field" : "20"},
        {"field" : "20"},
        {"field" : "20"}
        ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: table a | filldown field, random_Field") {
    val actual = execute("""| filldown field, random_Field | table field, random_Field""")
    val expected =
      """[
          {"field":"10","random_Field":"100"},
          {"field":"10","random_Field":"100"},
          {"field":"10","random_Field":"150"},
          {"field":"10","random_Field":"150"},
          {"field":"10","random_Field":"150"},
          {"field":"20","random_Field":"830"},
          {"field":"20","random_Field":"830"},
          {"field":"20","random_Field":"830"}
        ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4. Command: table a | filldown field by ID") {
    val actual = execute("""| filldown field by ID | table field, ID """)
    val expected = """[
    {"field":"10","ID":"0"},
    {"field":"10","ID":"0"},
    {"field":"10","ID":"0"},
    {"field":"10","ID":"0"},
    {"ID":"1"},
    {"field":"20","ID":"1"},
    {"field":"20","ID":"1"},
    {"field":"20","ID":"1"}
    ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Command: table a | filldown field, random_Field by ID") {
    val actual = execute("""| filldown field, random_Field by ID | table field, random_Field, ID""")
    val expected = """[
    {"field":"10","random_Field":"100","ID":"0"},
    {"field":"10","random_Field":"100","ID":"0"},
    {"field":"10","random_Field":"150","ID":"0"},
    {"field":"10","random_Field":"150","ID":"0"},
    {"ID":"1"},
    {"field":"20","random_Field":"830","ID":"1"},
    {"field":"20","random_Field":"830","ID":"1"},
    {"field":"20","random_Field":"830","ID":"1"}
    ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}