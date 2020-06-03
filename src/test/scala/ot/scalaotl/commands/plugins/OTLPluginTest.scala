package ot.scalaotl.commands.plugins

import ot.scalaotl.commands.CommandTest

class OTLPluginTest extends CommandTest {
  test("Test 1. Command: | Plugin system testing") {
    val actual = execute("""demo""")
        val expected = """[
                         |{"default_column":"src/test/resources/plugins/demo-plugin"}
                         |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
