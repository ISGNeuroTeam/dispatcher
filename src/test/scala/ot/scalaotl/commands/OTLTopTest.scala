package ot.scalaotl.commands

class OTLTopTest extends CommandTest {

  override val dataset: String = """[
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\", \"second_Field\": \"aa\"}","_nifi_time":"1568037188486","serialField":"0","random_Field":"100","WordField":"qwe","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\", \"second_Field\": \"bb\"}","_nifi_time":"1568037188487","serialField":"1","random_Field":"-90","WordField":"rty","junkField":"132_.","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\", \"second_Field\": \"cc\"}","_nifi_time":"1568037188487","serialField":"2","random_Field":"50","WordField":"uio","junkField":"asd.cx","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\", \"second_Field\": \"aa\"}","_nifi_time":"1568037188487","serialField":"3","random_Field":"20","WordField":"GreenPeace","junkField":"XYZ","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"4\", \"random_Field\": \"20\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\", \"second_Field\": \"aa\"}","_nifi_time":"1568037188487","serialField":"4","random_Field":"20","WordField":"fgh","junkField":"123_ASD","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\", \"second_Field\": \"cc\"}","_nifi_time":"1568037188487","serialField":"5","random_Field":"50","WordField":"jkl","junkField":"casd(@#)asd","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\", \"second_Field\": \"aa\"}","_nifi_time":"1568037188487","serialField":"6","random_Field":"60","WordField":"zxc","junkField":"QQQ.2","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"7\", \"random_Field\": \"50\", \"WordField\": \"RUS\", \"junkField\": \"00_3\", \"second_Field\": \"cc\"}","_nifi_time":"1568037188487","serialField":"7","random_Field":"50","WordField":"RUS","junkField":"00_3","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\", \"second_Field\": \"dd\"}","_nifi_time":"1568037188487","serialField":"8","random_Field":"0","WordField":"MMM","junkField":"112","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"9","random_Field":"10","WordField":"USA","junkField":"word","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"80\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"10","random_Field":"80","WordField":"USA","junkField":"RRR","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"70\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"11","random_Field":"70","WordField":"USA","junkField":"qwerty","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"110\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"12","random_Field":"110","WordField":"USA","junkField":"12334t","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"-40\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"13","random_Field":"-40","WordField":"USA","junkField":"r8u","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"8\", \"random_Field\": \"5\", \"WordField\": \"MMM\", \"junkField\": \"112\", \"second_Field\": \"dd\"}","_nifi_time":"1568037188487","serialField":"14","random_Field":"5","WordField":"MMM","junkField":"112","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"15","random_Field":"10","WordField":"USA","junkField":"word","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"50\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"16","random_Field":"50","WordField":"USA","junkField":"space","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"30\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"17","random_Field":"30","WordField":"USA","junkField":"two spieces","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"120\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"18","random_Field":"120","WordField":"USA","junkField":"one piece","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
    {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"-50\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"19","random_Field":"-50","WordField":"USA","junkField":"Amo","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"}
    ]"""

  test("Test 1. Command: | top random_Field ") {
    val actual = execute("""top random_Field """)
    val expected = """[
                     |{"random_Field":"50","count":4,"percent":20.0},
                     |{"random_Field":"20","count":2,"percent":10.0},
                     |{"random_Field":"10","count":2,"percent":10.0},
                     |{"random_Field":"-90","count":1,"percent":5.0},
                     |{"random_Field":"30","count":1,"percent":5.0},
                     |{"random_Field":"0","count":1,"percent":5.0},
                     |{"random_Field":"110","count":1,"percent":5.0},
                     |{"random_Field":"5","count":1,"percent":5.0},
                     |{"random_Field":"100","count":1,"percent":5.0},
                     |{"random_Field":"70","count":1,"percent":5.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | top <num> <field>") {
    val actual = execute("""top 4 random_Field""")
    val expected = """[
                     |{"random_Field":"50","count":4,"percent":20.0},
                     |{"random_Field":"20","count":2,"percent":10.0},
                     |{"random_Field":"10","count":2,"percent":10.0},
                     |{"random_Field":"-90","count":1,"percent":5.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | top random_Field second_Field ") {
    val actual = execute("""top random_Field second_Field""")
    val expected = """[
                     |{"random_Field":"50","second_Field":"cc","count":3,"percent":15.0},
                     |{"random_Field":"20","second_Field":"aa","count":2,"percent":10.0},
                     |{"random_Field":"10","second_Field":"ee","count":2,"percent":10.0},
                     |{"random_Field":"110","second_Field":"ee","count":1,"percent":5.0},
                     |{"random_Field":"60","second_Field":"aa","count":1,"percent":5.0},
                     |{"random_Field":"50","second_Field":"ee","count":1,"percent":5.0},
                     |{"random_Field":"30","second_Field":"ee","count":1,"percent":5.0},
                     |{"random_Field":"-50","second_Field":"ee","count":1,"percent":5.0},
                     |{"random_Field":"100","second_Field":"aa","count":1,"percent":5.0},
                     |{"random_Field":"80","second_Field":"ee","count":1,"percent":5.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  // проблема с бэктиками
  ignore("Test 4. Command: | top random_Field by _time") {
    val actual = execute("""top random_Field by _time""")
    val expected = """[
                     |{"random_Field":"50","second_Field":"cc","count":3,"percent":30.0},
                     |{"random_Field":"20","second_Field":"aa","count":2,"percent":20.0},
                     |{"random_Field":"60","second_Field":"aa","count":1,"percent":10.0},
                     |{"random_Field":"100","second_Field":"aa","count":1,"percent":10.0},
                     |{"random_Field":"-90","second_Field":"bb","count":1,"percent":10.0},
                     |{"random_Field":"10","second_Field":"ee","count":1,"percent":10.0},
                     |{"random_Field":"0","second_Field":"dd","count":1,"percent":10.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  // проблема с бэктиками
  ignore("Test 5. Command: | top random_Field by second_Field") {
    val actual = execute("""top random_Field by second_Field""")
    val expected = """[
                     |{"random_Field":"50","second_Field":"cc","count":3,"percent":30.0},
                     |{"random_Field":"20","second_Field":"aa","count":2,"percent":20.0},
                     |{"random_Field":"60","second_Field":"aa","count":1,"percent":10.0},
                     |{"random_Field":"100","second_Field":"aa","count":1,"percent":10.0},
                     |{"random_Field":"-90","second_Field":"bb","count":1,"percent":10.0},
                     |{"random_Field":"10","second_Field":"ee","count":1,"percent":10.0},
                     |{"random_Field":"0","second_Field":"dd","count":1,"percent":10.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 6. Command: | top <wrong_field>") {
    val actual = execute("""top wrong_field""")
    val expected = """[
                     |{"count":20,"percent":100.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  //По идее, когда ставишь "top 0 fieldname" то запрос должен выводить все результаты (а не 10 по-умолчанию), но при 0, он выдает просто пустой датафрейм.
  ignore("Test 7. Command: | top 0 <field>") {
    val actual = execute("""top 0 random_Field""")
    val expected = """[
                     |{"random_Field":"50","count":4,"percent":20.0},
                     |{"random_Field":"20","count":2,"percent":10.0},
                     |{"random_Field":"10","count":2,"percent":10.0},
                     |{"random_Field":"-90","count":1,"percent":5.0},
                     |{"random_Field":"30","count":1,"percent":5.0},
                     |{"random_Field":"0","count":1,"percent":5.0},
                     |{"random_Field":"110","count":1,"percent":5.0},
                     |{"random_Field":"5","count":1,"percent":5.0},
                     |{"random_Field":"100","count":1,"percent":5.0},
                     |{"random_Field":"70","count":1,"percent":5.0},
                     |{"random_Field":"120","count":1,"percent":5.0},
                     |{"random_Field":"-50","count":1,"percent":5.0},
                     |{"random_Field":"60","count":1,"percent":5.0},
                     |{"random_Field":"-40","count":1,"percent":5.0},
                     |{"random_Field":"80","count":1,"percent":5.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}


