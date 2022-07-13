package ot.scalaotl.commands

class OTLTransactionTest extends CommandTest {

  // вообще не работает
  ignore("Test 1. Command: | transaction <field>") {
    val actual = execute(""" transaction random_Field""")
    val expected =
      """[

        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}