package ot.scalaotl.commands

class OTLFillnullTest extends CommandTest {
  override val dataset: String = """[
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\", \"nullableColumn.1\" : 25}","_nifi_time":"1568037188486","serialField":"0","random_Field":"100","WordField":"qwe","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751", "nullableColumn.1" : 25 },
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_nifi_time":"1568037188487","serialField":"1","random_Field":"-90","WordField":"rty","junkField":"132_.","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_nifi_time":"1568037188487","serialField":"2","random_Field":"50","WordField":"uio","junkField":"asd.cx","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_nifi_time":"1568037188487","serialField":"3","random_Field":"20","WordField":"GreenPeace","junkField":"XYZ","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_nifi_time":"1568037188487","serialField":"4","random_Field":"30","WordField":"fgh","junkField":"123_ASD","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_nifi_time":"1568037188487","serialField":"5","random_Field":"50","WordField":"jkl","junkField":"casd(@#)asd","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_nifi_time":"1568037188487","serialField":"6","random_Field":"60","WordField":"zxc","junkField":"QQQ.2","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_nifi_time":"1568037188487","serialField":"7","random_Field":"-100","WordField":"RUS","junkField":"00_3","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_nifi_time":"1568037188487","serialField":"8","random_Field":"0","WordField":"MMM","junkField":"112","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_nifi_time":"1568037188487","serialField":"9","random_Field":"10","WordField":"USA","junkField":"word","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"}
                          |]"""

  test("Test 0. Command: | fillnull") {
    val actual = execute("""|table abc.qw |fillnull value=1 abc.qw""")
    val expected = """[
                     |{"abc.qw":"1"},
                     |{"abc.qw":"1"},
                     |{"abc.qw":"1"},
                     |{"abc.qw":"1"},
                     |{"abc.qw":"1"},
                     |{"abc.qw":"1"},
                     |{"abc.qw":"1"},
                     |{"abc.qw":"1"},
                     |{"abc.qw":"1"},
                     |{"abc.qw":"1"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | fillnull") {
    val actual = execute("""table nullableColumn.1 | eval x=tonumber('nullableColumn.1') | fillnull value=1.2 x  """)
    val expected = """[
                     |{"nullableColumn.1":"25","x":25.0},
                     |{"x":1.2},
                     |{"x":1.2},
                     |{"x":1.2},
                     |{"x":1.2},
                     |{"x":1.2},
                     |{"x":1.2},
                     |{"x":1.2},
                     |{"x":1.2},
                     |{"x":1.2}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 2. Command: | fillnull") {
    val actual = execute("""table nullableColumn.1 | eval x=tonumber('nullableColumn.1') |fillnull value="abc" x""")
    val expected =
      """[
        |{"nullableColumn.1":"25","x":25.0},
        |{},
        |{},
        |{},
        |{},
        |{},
        |{},
        |{},
        |{},
        |{}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | fillnull") {
    val actual = execute("""| fillnull value=100 qq.ww.e[0].tt qq.ww.e[1].tt qq.ww.e[2].tt eer err""")
    val expected = """[
                     |{"qq.ww.e[1].tt":"100","err":"100","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\", \"nullableColumn.1\" : 25}","eer":"100","_time":1568026476854,"qq.ww.e[2].tt":"100","qq.ww.e[0].tt":"100"},
                     |{"qq.ww.e[1].tt":"100","err":"100","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","eer":"100","_time":1568026476854,"qq.ww.e[2].tt":"100","qq.ww.e[0].tt":"100"},
                     |{"qq.ww.e[1].tt":"100","err":"100","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","eer":"100","_time":1568026476854,"qq.ww.e[2].tt":"100","qq.ww.e[0].tt":"100"},
                     |{"qq.ww.e[1].tt":"100","err":"100","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","eer":"100","_time":1568026476854,"qq.ww.e[2].tt":"100","qq.ww.e[0].tt":"100"},
                     |{"qq.ww.e[1].tt":"100","err":"100","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","eer":"100","_time":1568026476854,"qq.ww.e[2].tt":"100","qq.ww.e[0].tt":"100"},
                     |{"qq.ww.e[1].tt":"100","err":"100","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","eer":"100","_time":1568026476854,"qq.ww.e[2].tt":"100","qq.ww.e[0].tt":"100"},
                     |{"qq.ww.e[1].tt":"100","err":"100","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","eer":"100","_time":1568026476854,"qq.ww.e[2].tt":"100","qq.ww.e[0].tt":"100"},
                     |{"qq.ww.e[1].tt":"100","err":"100","_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","eer":"100","_time":1568026476854,"qq.ww.e[2].tt":"100","qq.ww.e[0].tt":"100"},
                     |{"qq.ww.e[1].tt":"100","err":"100","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","eer":"100","_time":1568026476854,"qq.ww.e[2].tt":"100","qq.ww.e[0].tt":"100"},
                     |{"qq.ww.e[1].tt":"100","err":"100","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","eer":"100","_time":1568026476854,"qq.ww.e[2].tt":"100","qq.ww.e[0].tt":"100"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
