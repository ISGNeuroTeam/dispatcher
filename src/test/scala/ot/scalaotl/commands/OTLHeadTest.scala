package ot.scalaotl.commands

class OTLHeadTest extends CommandTest {

  test("Test 0. Command: | head 1") {
    val actual = execute("""head 1""")
    val expected = """[{
                     |  "_time": 1568026476854,
                     |  "_raw": "{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}"
                     |}]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
