package ot.scalaotl.commands

class OTLLookupTest extends CommandTest {
  override val dataset: String = """[
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField.1\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_nifi_time":"1568037188486","serialField.1":"0","random_Field":"100","WordField":"qwe","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751"},
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
  test("Test 0. Command: | lookup name.csv col1 as col2") {
    val lookup ="""col1,col2
                  |a,1
                  |b,2""".stripMargin
    writeTextFile(lookup, "lookups/testlookup0.csv")

    val actual = execute("""lookup testlookup0.csv col2 as serialField.1""")
    val expected = """[
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField.1":"5","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField.1":"7","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField.1":"2","col1":["b"]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField.1":"9","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField.1":"8","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField.1":"6","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField.1":"3","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField.1":"1","col1":["a"]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","serialField.1":"0","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField.1":"4","col1":[]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected),f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | lookup name.csv col1") {
    val lookup ="""col1,col2
                  |a,1
                  |b,2""".stripMargin
    writeTextFile(lookup, "lookups/testlookup1.csv")

    val actual = execute("""lookup testlookup1.csv col1""")
    val expected = """[
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","col2":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","col2":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","col2":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","col2":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","col2":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","col2":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","col2":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","col2":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","col2":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","col2":[]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected),f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | lookup name.csv col1") {
    val lookup ="""col1,serialField.1
                  |a,1
                  |b,2""".stripMargin
    writeTextFile(lookup, "lookups/testlookup2.csv")

    val actual = execute("""lookup testlookup2.csv serialField.1""")
    val expected = """[
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField.1":"5","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField.1":"7","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField.1":"2","col1":["b"]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField.1":"9","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField.1":"8","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField.1":"6","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField.1":"3","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField.1":"1","col1":["a"]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","serialField.1":"0","col1":[]},
                     |{"_time":1568026476854,"_raw":"{\"serialField.1\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField.1":"4","col1":[]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected),f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | working with fields inserted by lookup command") {
    val actual = execute("""makeresults
                           || eval a=1, b=2
                           || table - _time
                           || otoutputlookup stas_bug_lookup_1.csv
                           || makeresults
                           || eval a=1, c=3
                           || table - _time
                           || otoutputlookup stas_bug_lookup_2.csv
                           || otinputlookup stas_bug_lookup_1.csv
                           || lookup stas_bug_lookup_2.csv a
                           || eval d = c
                           """.stripMargin)
    val expected = """[
                     |{"a":1,"b":2,"c":[3],"d":[3]}
                     |]
                     |""".stripMargin

    assert(jsonCompare(actual, expected),f"Result : $actual\n---\nExpected : $expected")
  }

}
