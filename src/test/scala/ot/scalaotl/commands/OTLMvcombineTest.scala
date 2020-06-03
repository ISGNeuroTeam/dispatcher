package ot.scalaotl.commands

class OTLMvcombineTest extends CommandTest {
  override val dataset: String = """[
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","ho.st":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\", \"ho.st\":\"test.local:9990\"}","_nifi_time":"1568037188486","serialField":"0","random_Field":"100","WordField":"qwe","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","ho.st":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\", \"ho.st\":\"test.local:9990\"}","_nifi_time":"1568037188487","serialField":"1","random_Field":"-90","WordField":"rty","junkField":"132_.","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","ho.st":"test.local1:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\",\"ho.st\":\"test.local1:9990\"}","_nifi_time":"1568037188487","serialField":"1","random_Field":"-90","WordField":"rty","junkField":"132_.","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"}
                          |]"""

  test("Test 0. Command: | mvcombine ho.st ") {
    val actual = execute("""table random_Field index ho.st | mvcombine ho.st """)
    val expected =
      """[
        |{"random_Field":"-90","index":"test_index-OTLMvcombineTest","ho.st":["test.local:9990","test.local1:9990"]},
        |{"random_Field":"100","index":"test_index-OTLMvcombineTest","ho.st":["test.local:9990"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
