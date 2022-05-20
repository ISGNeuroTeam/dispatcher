package ot.scalaotl.commands

class OTLNomvTest extends CommandTest {

  override val dataset = """[
                             |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_nifi_time":"1568037188486","serialField":"0","random_Field":"100","WordField":"qwe","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751"}
                             |]"""

  test("Test 1. Command: |  nomv <field> ") {
    val actual = execute("""eval t=mvrange(0,9) | nomv t""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","t":"0.0 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}