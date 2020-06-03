package ot.scalaotl.commands

class OTLAddtotalsTest extends CommandTest {
  override val dataset: String = """[
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField.1\": \"0\", \"random_Field\": \"100\", \"WordField\": \"1\", \"junkField\": \"q2W\"}","_nifi_time":"1568037188486","serialField.1":"0","random_Field":"100","WordField":"1","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField.1\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_nifi_time":"1568037188487","serialField.1":"1","random_Field":"-90","WordField":"rty","junkField":"132_.","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField.1\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_nifi_time":"1568037188487","serialField.1":"2","random_Field":"50","WordField":"uio","junkField":"asd.cx","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField.1\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_nifi_time":"1568037188487","serialField.1":"3","random_Field":"20","WordField":"GreenPeace","junkField":"XYZ","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField.1\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_nifi_time":"1568037188487","serialField.1":"4","random_Field":"30","WordField":"fgh","junkField":"123_ASD","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField.1\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_nifi_time":"1568037188487","serialField.1":"5","random_Field":"50","WordField":"jkl","junkField":"casd(@#)asd","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField.1\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_nifi_time":"1568037188487","serialField.1":"6","random_Field":"60","WordField":"zxc","junkField":"QQQ.2","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField.1\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_nifi_time":"1568037188487","serialField.1":"7","random_Field":"-100","WordField":"RUS","junkField":"00_3","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField.1\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_nifi_time":"1568037188487","serialField.1":"8","random_Field":"0","WordField":"MMM","junkField":"112","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField.1\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_nifi_time":"1568037188487","serialField.1":"9","random_Field":"10","WordField":"USA","junkField":"word","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"}
                          |]"""

  test("Test 0. Command: |  addtotals") {
    val actual = execute("""addtotals random_Field serialField.1 WordField""")
    val expected = """[
                     |{"WordField":"1","_raw":"{\"serialField.1\": \"0\", \"random_Field\": \"100\", \"WordField\": \"1\", \"junkField\": \"q2W\"}","serialField.1":"0","random_Field":"100","_time":1568026476854,"Total":101.0},
                     |{"WordField":"rty","_raw":"{\"serialField.1\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField.1":"1","random_Field":"-90","_time":1568026476854},
                     |{"WordField":"uio","_raw":"{\"serialField.1\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField.1":"2","random_Field":"50","_time":1568026476854},
                     |{"WordField":"GreenPeace","_raw":"{\"serialField.1\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField.1":"3","random_Field":"20","_time":1568026476854},
                     |{"WordField":"fgh","_raw":"{\"serialField.1\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField.1":"4","random_Field":"30","_time":1568026476854},
                     |{"WordField":"jkl","_raw":"{\"serialField.1\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField.1":"5","random_Field":"50","_time":1568026476854},
                     |{"WordField":"zxc","_raw":"{\"serialField.1\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField.1":"6","random_Field":"60","_time":1568026476854},
                     |{"WordField":"RUS","_raw":"{\"serialField.1\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField.1":"7","random_Field":"-100","_time":1568026476854},
                     |{"WordField":"MMM","_raw":"{\"serialField.1\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField.1":"8","random_Field":"0","_time":1568026476854},
                     |{"WordField":"USA","_raw":"{\"serialField.1\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField.1":"9","random_Field":"10","_time":1568026476854}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: |  addtotals col=true") {
    val actual = execute("""addtotals col=true random_Field serialField.1 WordField""")
    val expected = """[
                     |{"WordField":"1","_raw":"{\"serialField.1\": \"0\", \"random_Field\": \"100\", \"WordField\": \"1\", \"junkField\": \"q2W\"}","serialField.1":"0","random_Field":"100","_time":1568026476854,"Total":101.0},
                     |{"WordField":"rty","_raw":"{\"serialField.1\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField.1":"1","random_Field":"-90","_time":1568026476854},
                     |{"WordField":"uio","_raw":"{\"serialField.1\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField.1":"2","random_Field":"50","_time":1568026476854},
                     |{"WordField":"GreenPeace","_raw":"{\"serialField.1\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField.1":"3","random_Field":"20","_time":1568026476854},
                     |{"WordField":"fgh","_raw":"{\"serialField.1\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField.1":"4","random_Field":"30","_time":1568026476854},
                     |{"WordField":"jkl","_raw":"{\"serialField.1\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField.1":"5","random_Field":"50","_time":1568026476854},
                     |{"WordField":"zxc","_raw":"{\"serialField.1\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField.1":"6","random_Field":"60","_time":1568026476854},
                     |{"WordField":"RUS","_raw":"{\"serialField.1\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField.1":"7","random_Field":"-100","_time":1568026476854},
                     |{"WordField":"MMM","_raw":"{\"serialField.1\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField.1":"8","random_Field":"0","_time":1568026476854},
                     |{"WordField":"USA","_raw":"{\"serialField.1\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField.1":"9","random_Field":"10","_time":1568026476854},
                     |{"WordField":"1.0","serialField.1":"45.0","random_Field":"130.0","Total":101.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: |  addtotals col=true row=false") {
    val actual = execute("""addtotals col=true row=false random_Field serialField.1 WordField""")
    val expected = """ [
                     |{"WordField":"1","_raw":"{\"serialField.1\": \"0\", \"random_Field\": \"100\", \"WordField\": \"1\", \"junkField\": \"q2W\"}","serialField.1":"0","random_Field":"100","_time":1568026476854},
                     |{"WordField":"rty","_raw":"{\"serialField.1\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField.1":"1","random_Field":"-90","_time":1568026476854},
                     |{"WordField":"uio","_raw":"{\"serialField.1\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField.1":"2","random_Field":"50","_time":1568026476854},
                     |{"WordField":"GreenPeace","_raw":"{\"serialField.1\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField.1":"3","random_Field":"20","_time":1568026476854},
                     |{"WordField":"fgh","_raw":"{\"serialField.1\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField.1":"4","random_Field":"30","_time":1568026476854},
                     |{"WordField":"jkl","_raw":"{\"serialField.1\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField.1":"5","random_Field":"50","_time":1568026476854},
                     |{"WordField":"zxc","_raw":"{\"serialField.1\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField.1":"6","random_Field":"60","_time":1568026476854},
                     |{"WordField":"RUS","_raw":"{\"serialField.1\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField.1":"7","random_Field":"-100","_time":1568026476854},
                     |{"WordField":"MMM","_raw":"{\"serialField.1\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField.1":"8","random_Field":"0","_time":1568026476854},
                     |{"WordField":"USA","_raw":"{\"serialField.1\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField.1":"9","random_Field":"10","_time":1568026476854},
                     |{"WordField":"1.0","serialField.1":"45.0","random_Field":"130.0"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: |  addtotals col=true labelfield=WordField label=all") {
    val actual = execute("""addtotals col=true labelfield=WordField label=all random_Field serialField.1 WordField""")
    val expected = """[
                     |{"_raw":"{\"serialField.1\": \"0\", \"random_Field\": \"100\", \"WordField\": \"1\", \"junkField\": \"q2W\"}","serialField.1":"0","random_Field":"100","_time":1568026476854,"Total":101.0},
                     |{"_raw":"{\"serialField.1\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField.1":"1","random_Field":"-90","_time":1568026476854},
                     |{"_raw":"{\"serialField.1\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField.1":"2","random_Field":"50","_time":1568026476854},
                     |{"_raw":"{\"serialField.1\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField.1":"3","random_Field":"20","_time":1568026476854},
                     |{"_raw":"{\"serialField.1\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField.1":"4","random_Field":"30","_time":1568026476854},
                     |{"_raw":"{\"serialField.1\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField.1":"5","random_Field":"50","_time":1568026476854},
                     |{"_raw":"{\"serialField.1\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField.1":"6","random_Field":"60","_time":1568026476854},
                     |{"_raw":"{\"serialField.1\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField.1":"7","random_Field":"-100","_time":1568026476854},
                     |{"_raw":"{\"serialField.1\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField.1":"8","random_Field":"0","_time":1568026476854},
                     |{"_raw":"{\"serialField.1\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField.1":"9","random_Field":"10","_time":1568026476854},
                     |{"WordField":"all","serialField.1":"45.0","random_Field":"130.0","Total":101.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
  test("Test 4. Command: |  addtotals col=true row=false labelfield=WordField label=all") {
    val actual = execute("""addtotals col=true row=false labelfield=WordField label=all random_Field serialField.1 WordField""")
    val expected = """[
                     |{"WordField":"all","_raw":"{\"serialField.1\": \"0\", \"random_Field\": \"100\", \"WordField\": \"1\", \"junkField\": \"q2W\"}","serialField.1":"0","random_Field":"100","_time":1568026476854},
                     |{"WordField":"all","_raw":"{\"serialField.1\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField.1":"1","random_Field":"-90","_time":1568026476854},
                     |{"WordField":"all","_raw":"{\"serialField.1\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField.1":"2","random_Field":"50","_time":1568026476854},
                     |{"WordField":"all","_raw":"{\"serialField.1\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField.1":"3","random_Field":"20","_time":1568026476854},
                     |{"WordField":"all","_raw":"{\"serialField.1\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField.1":"4","random_Field":"30","_time":1568026476854},
                     |{"WordField":"all","_raw":"{\"serialField.1\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField.1":"5","random_Field":"50","_time":1568026476854},
                     |{"WordField":"all","_raw":"{\"serialField.1\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField.1":"6","random_Field":"60","_time":1568026476854},
                     |{"WordField":"all","_raw":"{\"serialField.1\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField.1":"7","random_Field":"-100","_time":1568026476854},
                     |{"WordField":"all","_raw":"{\"serialField.1\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField.1":"8","random_Field":"0","_time":1568026476854},
                     |{"WordField":"all","_raw":"{\"serialField.1\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField.1":"9","random_Field":"10","_time":1568026476854},
                     |{"WordField":"all","serialField.1":"45.0","random_Field":"130.0"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
