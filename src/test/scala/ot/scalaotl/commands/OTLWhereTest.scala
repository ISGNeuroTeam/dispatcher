package ot.scalaotl.commands

class OTLWhereTest extends CommandTest {

  test("Test 0. Command: |  where field > value") {
    val actual = execute("""table _raw, _time, serialField | where serialField > 5"""")//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6"},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7"},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8"},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 0.1. Command: |  where field >= value") {
    val actual = execute("""table _raw, _time, serialField | where serialField >= 5""")//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5"},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6"},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7"},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8"},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: |  where field = mvalue") {
    val actual = execute("""  eval rng=mvrange(1,10,2) | eval sf=tonumber(serialField) | where sf=rng    """)//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","rng":[1.0,3.0,5.0,7.0,9.0],"sf":1.0,"serialField":"1","_time":1568026476855},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","rng":[1.0,3.0,5.0,7.0,9.0],"sf":3.0,"serialField":"3","_time":1568026476857},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","rng":[1.0,3.0,5.0,7.0,9.0],"sf":5.0,"serialField":"5","_time":1568026476859},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","rng":[1.0,3.0,5.0,7.0,9.0],"sf":7.0,"serialField":"7","_time":1568026476861},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","rng":[1.0,3.0,5.0,7.0,9.0],"sf":9.0,"serialField":"9","_time":1568026476863}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: |  where mvalue != field") {
    val actual = execute("""  eval rng=mvrange(1,10,2) | eval sf=tonumber(serialField) | where rng!=sf  """)//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","rng":[1.0,3.0,5.0,7.0,9.0],"sf":0.0,"serialField":"0","_time":1568026476854},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","rng":[1.0,3.0,5.0,7.0,9.0],"sf":2.0,"serialField":"2","_time":1568026476856},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","rng":[1.0,3.0,5.0,7.0,9.0],"sf":4.0,"serialField":"4","_time":1568026476858},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","rng":[1.0,3.0,5.0,7.0,9.0],"sf":6.0,"serialField":"6","_time":1568026476860},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","rng":[1.0,3.0,5.0,7.0,9.0],"sf":8.0,"serialField":"8","_time":1568026476862}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: |  where mvalue > field") {
    val actual = execute("""  eval rng=mvrange(1,10,2) | eval sf=tonumber(serialField) | where rng>sf  """)//TODO
    val expected = """[]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 4. Command: |  where mvalue = mvalue") {
    val actual = execute("""  eval rng1=mvrange(1,10,2) |eval rng2=mvrange(1,10,2)| where rng1=rng2  """)//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0,7.0,9.0]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Command: |  where mvalue != mvalue") {
    val actual = execute("""  eval rng1=mvrange(1,10,2) |eval rng2=mvrange(1,5,2)  | where rng1!=rng2  """)//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0]},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0]},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0]},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0]},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0]},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0]},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0]},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0]},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0]},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"rng1":[1.0,3.0,5.0,7.0,9.0],"rng2":[1.0,3.0,5.0]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 6. Command: |  where mvalue > mvalue") {
    val actual = execute("""  eval rng1=mvrange(1,10,2) |eval rng2=mvrange(1,5,2)  | where rng1 > rng2  """)//TODO
    val expected = """[]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 7. Command: |  where mvfield = value") {
    val actual = execute("""stats values(WordField) as wf | where wf="zxc" """)//TODO
    val expected = """[
                     |{"wf":["zxc","MMM","GreenPeace","uio","rty","RUS","USA","qwe","jkl","fgh"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 8. Command: |  where field !=\"\"") {
    val actual = execute(""" where serialField !="" """)//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0"},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"serialField":"1"},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"serialField":"2"},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"serialField":"3"},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"serialField":"4"},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5"},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6"},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7"},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8"},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 9. Command: |  where field !=\"\"") {
    val actual = execute("""eval sf=tonumber(serialField) | where sf !="" """)//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0","sf":0.0},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"serialField":"1","sf":1.0},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"serialField":"2","sf":2.0},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"serialField":"3","sf":3.0},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"serialField":"4","sf":4.0},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5","sf":5.0},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6","sf":6.0},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7","sf":7.0},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8","sf":8.0},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9","sf":9.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 9.1 Command: |  where field !=\"\"") {
    val actual = execute("""eval rf=tonumber(random_Field) | where rf > 50 """)//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"random_Field":"100","rf":100.0},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"random_Field":"60","rf":60.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 10. Command: |  where mvfield != \"\"") {
    val actual = execute("""stats values(WordField) as wf | where wf!="" """)//TODO
    val expected = """[
                     |{"wf":["zxc","MMM","GreenPeace","uio","rty","RUS","USA","qwe","jkl","fgh"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 11. Command: |  where mvfield != \"\"") {
    val actual = execute("""eval sf=tonumber(serialField) |stats values(sf) as msf | where msf!="" """)//TODO
    val expected = """[
                     |{"msf":[0.0,2.0,9.0,5.0,8.0,1.0,4.0,6.0,3.0,7.0]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 12.0 Command: |  where AND ") {
    val actual = execute("""eval sf=tonumber(serialField) |stats values(sf) as msf | eval msf2=msf| where ((msf!="") AND (msf2=4)) """)//TODO
    val expected = """[
                     |{"msf":[0.0,2.0,9.0,5.0,8.0,1.0,4.0,6.0,3.0,7.0],"msf2":[0.0,2.0,9.0,5.0,8.0,1.0,4.0,6.0,3.0,7.0]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 12.1 Command: |  where AND") {
    val actual = execute("""eval sf=tonumber(serialField) |stats values(sf) as msf | eval msf2=msf| where msf!="" AND msf2!=4 """)//TODO
    val expected = """[]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 13 Command: |  where eval(mv)=eval(sv1) AND mv=sv2") {
    val actual = execute("""eval sf=tonumber(serialField) |stats values(sf) as msf | eval msf2=msf| where mvindex(msf,0, 6)!=tonumber("4") AND (msf2!="") """)//TODO
    val expected = """[
                     |{"msf":[0.0,2.0,9.0,5.0,8.0,1.0,4.0,6.0,3.0,7.0],"msf2":[0.0,2.0,9.0,5.0,8.0,1.0,4.0,6.0,3.0,7.0]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 14. Command: |  where like(field, value)") {
    val actual = execute(""" where (( (like( WordField, "zx%" )) AND  serialField = 6)) """)//TODO
    val expected = """[
                     |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","WordField":"zxc","serialField":"6"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 15. Command: |  where BIGINT > INT") {
    val actual = execute(""" makeresults | eval a = 9876543210 | fields a | where a > 0 """)
    val expected = """[
                     |{"a":9876543210}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
