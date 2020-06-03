package ot.scalaotl.commands

class OTLFilldownTest extends CommandTest {
    override val dataset: String = """[
     {"_time": 0, "_raw": "{\"ID\":0,\"field\": 10}"},
     {"_time": 0, "_raw": "{\"ID\":0}"},
     {"_time": 0, "_raw": "{\"ID\":0}"},
     {"_time": 0, "_raw": "{\"ID\":0}"},
     {"_time": 0, "_raw": "{\"ID\":1}"},
     {"_time": 0, "_raw": "{\"ID\":1,\"field\": 20}"},
     {"_time": 0, "_raw": "{\"ID\":1}"},
     {"_time": 0, "_raw": "{\"ID\":1}"}
     ]"""

    test("Test 0. Command: table a | filldown field") {
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
    test("Test 1. Command: table a | filldown field by ID") {
      val actual = execute("""| filldown field by ID| table field, ID """)
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

}