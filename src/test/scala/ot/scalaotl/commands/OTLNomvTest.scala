package ot.scalaotl.commands

class OTLNomvTest extends CommandTest {

  override   val dataset = """[
                             |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_nifi_time":"1568037188486","serialField":"0","random_Field":"100","WordField":"qwe","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751"}
                             |]"""

  // Не верное преобразование.
  ignore("Test 1. Command: |  nomv <field> ") {
    val actual = execute("""eval t=mvrange(0,9) | nomv t""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","t":"0 1 2 3 4 5 6 7 8 9"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}