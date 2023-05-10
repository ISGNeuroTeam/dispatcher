package ot.scalaotl.commands

import ot.scalaotl.Converter

class OTLJoinTest extends CommandTest {

  /*override val dataset: String =
    """[
      |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","random_Field": "100", "_raw":"{\"serialField\": \"0\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_nifi_time":"1568037188486","serialField":"0","WordField":"qwe","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751"},
      |{"_time":1568026476855,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","random_Field": "-90", "_raw":"{\"serialField\": \"1\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_nifi_time":"1568037188487","serialField":"1","WordField":"rty","junkField":"132_.","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476856,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","random_Field": "50", "_raw":"{\"serialField\": \"2\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_nifi_time":"1568037188487","serialField":"2","WordField":"uio","junkField":"asd.cx","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476857,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source", "random_Field": "20", "_raw":"{\"serialField\": \"3\",\"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_nifi_time":"1568037188487","serialField":"3","WordField":"GreenPeace","junkField":"XYZ","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476858,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source", "random_Field": "30", "_raw":"{\"serialField\": \"4\",\"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_nifi_time":"1568037188487","serialField":"4","WordField":"fgh","junkField":"123_ASD","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476859,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source", "random_Field": "50", "_raw":"{\"serialField\": \"5\",\"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_nifi_time":"1568037188487","serialField":"5","WordField":"jkl","junkField":"casd(@#)asd","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476860,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source", "random_Field": "60", "_raw":"{\"serialField\": \"6\",\"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_nifi_time":"1568037188487","serialField":"6","WordField":"zxc","junkField":"QQQ.2","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476861,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","random_Field": "-100", "_raw":"{\"serialField\": \"7\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_nifi_time":"1568037188487","serialField":"7","WordField":"RUS","junkField":"00_3","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476862,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","random_Field": "0", "_raw":"{\"serialField\": \"8\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_nifi_time":"1568037188487","serialField":"8","WordField":"MMM","junkField":"112","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476863,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source", "random_Field": "10", "_raw":"{\"serialField\": \"9\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_nifi_time":"1568037188487","serialField":"9","WordField":"USA","junkField":"word","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"}
      |]"""*/

  test("Test 0. Command: | join with out null columns") {

    val ssQuery = createQuery("eval random_Field=serialField*10 | table serialField, random_Field")
    val cacheDF = new Converter(ssQuery).run
    val cacheMap = Map("id1" -> cacheDF)

    val otlQuery = createQuery("table serialField, random_Field | join serialField subsearch=id1")
    val resultDF = new Converter(otlQuery, cacheMap).run


    val actual = resultDF.toJSON.collect().mkString("[\n", ",\n", "\n]")
    val expected =
      """[{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0","SF":0.0},
        |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"serialField":"1","SF":10.0},
        |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"serialField":"2","SF":20.0},
        |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"serialField":"3","SF":30.0},
        |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"serialField":"4","SF":40.0},
        |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5","SF":50.0},
        |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6","SF":60.0},
        |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7","SF":70.0},
        |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8","SF":80.0},
        |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9","SF":90.0}
        |]
        |""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | join with null columns") {

    val ssQuery = createQuery("eval SF=serialField*10 | table serialField, SF, NCol")
    val cacheDF = new Converter(ssQuery).run
    val cacheMap = Map("id1" -> cacheDF)

    val otlQuery = createQuery("join serialField subsearch=id1")
    val resultDF = new Converter(otlQuery, cacheMap).run


    val actual = resultDF.toJSON.collect().mkString("[\n", ",\n", "\n]")
    val expected =
      """[
        |{"serialField":"0","_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","SF":0.0},
        |{"serialField":"1","_time":1568026476854,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","SF":10.0},
        |{"serialField":"2","_time":1568026476854,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","SF":20.0},
        |{"serialField":"3","_time":1568026476854,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","SF":30.0},
        |{"serialField":"4","_time":1568026476854,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","SF":40.0},
        |{"serialField":"5","_time":1568026476854,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","SF":50.0},
        |{"serialField":"6","_time":1568026476854,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","SF":60.0},
        |{"serialField":"7","_time":1568026476854,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","SF":70.0},
        |{"serialField":"8","_time":1568026476854,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","SF":80.0},
        |{"serialField":"9","_time":1568026476854,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","SF":90.0}
        |]
        |""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | join with empty dataframe") {

    val ssQuery = createQuery("eval newfield=10 | where serialField=-1")
    val cacheDF = new Converter(ssQuery).run
    val cacheMap = Map("id1" -> cacheDF)

    val otlQuery = createQuery("join type=left serialField subsearch=id1")
    val resultDF = new Converter(otlQuery, cacheMap).run


    val actual = resultDF.toJSON.collect().mkString("[\n", ",\n", "\n]")
    val expected =
      """[
        |{"serialField":"0","_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}"},
        |{"serialField":"1","_time":1568026476854,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}"},
        |{"serialField":"2","_time":1568026476854,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}"},
        |{"serialField":"3","_time":1568026476854,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}"},
        |{"serialField":"4","_time":1568026476854,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}"},
        |{"serialField":"5","_time":1568026476854,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}"},
        |{"serialField":"6","_time":1568026476854,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}"},
        |{"serialField":"7","_time":1568026476854,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}"},
        |{"serialField":"8","_time":1568026476854,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}"},
        |{"serialField":"9","_time":1568026476854,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | join ") {
    val ssQuery = createQuery("table serialField| eval f1=1 | eval f2=2")
    val cacheDF = new Converter(ssQuery).run
    val cacheMap = Map("id1" -> cacheDF)

    val otlQuery = createQuery("join serialField subsearch=id1 | eval t3=f1 + f2")
    val resultDF = new Converter(otlQuery, cacheMap).run


    val actual = resultDF.toJSON.collect().mkString("[\n", ",\n", "\n]")
    val expected =
      """[
        |{"serialField":"0","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"f1":1,"f2":2,"t3":3},
        |{"serialField":"1","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"f1":1,"f2":2,"t3":3},
        |{"serialField":"2","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"f1":1,"f2":2,"t3":3},
        |{"serialField":"3","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"f1":1,"f2":2,"t3":3},
        |{"serialField":"4","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"f1":1,"f2":2,"t3":3},
        |{"serialField":"5","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"f1":1,"f2":2,"t3":3},
        |{"serialField":"6","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"f1":1,"f2":2,"t3":3},
        |{"serialField":"7","_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"f1":1,"f2":2,"t3":3},
        |{"serialField":"8","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"f1":1,"f2":2,"t3":3},
        |{"serialField":"9","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"f1":1,"f2":2,"t3":3}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}