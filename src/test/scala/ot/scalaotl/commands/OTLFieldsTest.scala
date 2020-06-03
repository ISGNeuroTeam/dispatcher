package ot.scalaotl.commands

class OTLFieldsTest extends CommandTest {

  test("Test 0. Command: | head 1") {
    val actual = execute("""fields - _raw""")
    val expected =
      """[
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854}
]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
