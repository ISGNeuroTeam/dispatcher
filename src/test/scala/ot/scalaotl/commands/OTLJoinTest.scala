package ot.scalaotl.commands

import ot.dispatcher.OTLQuery
import ot.scalaotl.Converter

class OTLJoinTest extends CommandTest {

  test("Test 0. Command: | join with out null columns") {

    val ssQuery = createQuery("eval SF=serialField*10 | table serialField, SF")
    val cacheDF = new Converter(ssQuery).run
    val cacheMap = Map("id1" -> cacheDF)

    val otlQuery = createQuery("join serialField subsearch=id1")
    val resultDF = new Converter(otlQuery, cacheMap).run


    val actual = resultDF.toJSON.collect().mkString("[\n", ",\n", "\n]")
    val expected =
      """[{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0","SF":0.0},
        |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476854,"serialField":"1","SF":10.0},
        |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476854,"serialField":"2","SF":20.0},
        |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476854,"serialField":"3","SF":30.0},
        |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476854,"serialField":"4","SF":40.0},
        |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476854,"serialField":"5","SF":50.0},
        |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476854,"serialField":"6","SF":60.0},
        |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476854,"serialField":"7","SF":70.0},
        |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476854,"serialField":"8","SF":80.0},
        |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476854,"serialField":"9","SF":90.0}
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
      """[{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0","SF":0.0},
        |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476854,"serialField":"1","SF":10.0},
        |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476854,"serialField":"2","SF":20.0},
        |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476854,"serialField":"3","SF":30.0},
        |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476854,"serialField":"4","SF":40.0},
        |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476854,"serialField":"5","SF":50.0},
        |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476854,"serialField":"6","SF":60.0},
        |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476854,"serialField":"7","SF":70.0},
        |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476854,"serialField":"8","SF":80.0},
        |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476854,"serialField":"9","SF":90.0}
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
        |{"serialField":"0","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854},
        |{"serialField":"1","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476854},
        |{"serialField":"2","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476854},
        |{"serialField":"3","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476854},
        |{"serialField":"4","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476854},
        |{"serialField":"5","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476854},
        |{"serialField":"6","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476854},
        |{"serialField":"7","_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476854},
        |{"serialField":"8","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476854},
        |{"serialField":"9","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476854}
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
        |{"serialField":"1","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476854,"f1":1,"f2":2,"t3":3},
        |{"serialField":"2","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476854,"f1":1,"f2":2,"t3":3},
        |{"serialField":"3","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476854,"f1":1,"f2":2,"t3":3},
        |{"serialField":"4","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476854,"f1":1,"f2":2,"t3":3},
        |{"serialField":"5","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476854,"f1":1,"f2":2,"t3":3},
        |{"serialField":"6","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476854,"f1":1,"f2":2,"t3":3},
        |{"serialField":"7","_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476854,"f1":1,"f2":2,"t3":3},
        |{"serialField":"8","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476854,"f1":1,"f2":2,"t3":3},
        |{"serialField":"9","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476854,"f1":1,"f2":2,"t3":3}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
