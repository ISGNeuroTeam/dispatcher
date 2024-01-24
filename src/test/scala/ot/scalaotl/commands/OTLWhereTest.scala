package ot.scalaotl.commands

class OTLWhereTest extends CommandTest {

  test("Test 0. Command: |  where field > value") {
    val actual = execute("""table _raw, _time, serialField | where serialField > 5""")//TODO
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

  test("Test 17. Command: | where field = false") {
    val actual = execute("""eval logicField = if(serialField>6, false, true) | where logicField = false""") //TODO
    val expected =
      """[
        {"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField":"7","logicField":false},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField":"8","logicField":false},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField":"9","logicField":false}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test ("Test 18. Command: | where field != false") {
    val actual = execute("""eval logicField = if(serialField>6, false, true) | where logicField != false""") //TODO
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","serialField":"0","logicField":true},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField":"1","logicField":true},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField":"2","logicField":true},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField":"3","logicField":true},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField":"4","logicField":true},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField":"5","logicField":true},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField":"6","logicField":true}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 19. Command: | where field = true") {
    val actual = execute("""eval logicField = if(random_Field>30, true, false) | where logicField = true""") //TODO
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","random_Field":"100","logicField":true},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","random_Field":"50","logicField":true},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","random_Field":"50","logicField":true},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","random_Field":"60","logicField":true}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 20. Command: | where field != true") {
    val actual = execute("""eval logicField = if(random_Field>30, true, false) | where logicField != true""") //TODO
    val expected =
      """[
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","random_Field":"-90","logicField":false},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","random_Field":"20","logicField":false},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","random_Field":"30","logicField":false},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","random_Field":"-100","logicField":false},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","random_Field":"0","logicField":false},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","random_Field":"10","logicField":false}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 21.1. Command: | where ()") {
    val actual = execute("""where (serialField < 4*2)""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","serialField":"0"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField":"1"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField":"2"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField":"3"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField":"4"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField":"5"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField":"6"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField":"7"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 21.2. Command: | where(())") {
    val actual = execute("""where (serialField > 2*(3+1))""")
    val expected =
      """[
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField":"9"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 22.1. Command: | where (() and ..)") {
    val actual = execute("""where (random_Field = 2 * (21 + 4) and serialField < 4)""")
    val expected =
      """[
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","random_Field":"50","serialField":"2"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 22.2. Command: | where ((()) and ..)") {
    val actual = execute("""where (random_Field > 2 * (21 + 4 * (10 - 7)) and serialField = 0)""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","random_Field":"100","serialField":"0"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 23.1. Command: | where (.. or ())") {
    val actual = execute("""where (random_Field = 30 or (serialField + 1) < 20 - (6 * 3))""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","random_Field":"100","serialField":"0"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","random_Field":"30","serialField":"4"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 23.2. Command: | where (.. or (()))") {
    val actual = execute("""where (serialField < 3 or (random_Field * (77 + 2)) > 3500)""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","serialField":"0","random_Field":"100"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField":"1","random_Field":"-90"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField":"2","random_Field":"50"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField":"5","random_Field":"50"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField":"6","random_Field":"60"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 24. Command: | where (field - 1) > () + ()") {
    val actual = execute("""where (serialField - 1) > (2 - 4) + (9 - 3)""")
    val expected =
      """[
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField":"6"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField":"7"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField":"8"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField":"9"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 25. Command: | where (()) = field - ()") {
    val actual = execute("""where (10 * (serialField - 4)) = random_Field - (10 + 30)""")
    val expected =
      """[
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField":"5","random_Field":"50"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField":"6","random_Field":"60"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 26. Command: | where () < ..") {
    val actual = execute("""where (random_Field - 100) < -70""")
    val expected =
      """[
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","random_Field":"-90"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","random_Field":"20"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","random_Field":"-100"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","random_Field":"0"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","random_Field":"10"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 27. Command: | where (()*()) > ..") {
    val actual = execute("""where ((random_Field + 50) * (25 + 10) + 1) > 4000""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","random_Field":"100"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","random_Field":"50"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","random_Field":"20"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","random_Field":"30"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","random_Field":"50"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","random_Field":"60"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","random_Field":"10"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 28.1. Command: | where .. = ()") {
    val actual = execute("""where random_Field = (serialField - 8)""")
    val expected =
      """[
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","random_Field":"0","serialField":"8"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 28.2. Command: | where .. = (())") {
    val actual = execute("""where serialField = ((22 - 19) + 4)""")
    val expected =
      """[
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField":"7"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 29.1. Command: | where (.. and (.. or ..))") {
    val actual = execute("""where (serialField > 3 and (random_Field = 0 or random_Field > 20))""")
    val expected =
      """[
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField":"4","random_Field":"30"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField":"5","random_Field":"50"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField":"6","random_Field":"60"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField":"8","random_Field":"0"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 29.2. Command: | where (.. or(.. or(.. or(.. or ..))))") {
    val actual = execute("""where (serialField = 1 or (random_Field = 50 or (random_Field = -100 or (serialField = 9 or serialField = 8))))""")
    val expected =
      """[
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField":"1","random_Field":"-90"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField":"2","random_Field":"50"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField":"5","random_Field":"50"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField":"7","random_Field":"-100"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField":"8","random_Field":"0"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField":"9","random_Field":"10"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 30.1. Command: | where ((.. or ..) and ..)") {
    val actual = execute("""where ((random_Field > 80 or random_Field = 50) and serialField < 5)""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","random_Field":"100","serialField":"0"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","random_Field":"50","serialField":"2"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 30.2. Command: | where ((((.. or ..) or ..) or ..) .. and)") {
    val actual = execute("""where ((((serialField = 2 or serialField = 7) or serialField = 4) or serialField = 6) and random_Field <= 60)""")
    val expected =
      """[
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField":"2","random_Field":"50"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField":"4","random_Field":"30"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField":"6","random_Field":"60"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField":"7","random_Field":"-100"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 31.1. Command with backspaces") {
    val actual = execute("""streamstats count | eval x=count*2 | eval z=x+1 | eval y = (-1) * count*3 | where ((x > 2 * ( 1 + 1  * (6 - (2+3)) ) and y < 6* (1 + (8-(3+2*2)))) or (  x=2  ) ) | fields count, x, z, y""")
    val expected =
      """[
        |{"count":1,"x":2,"z":3,"y":-3},
        |{"count":3,"x":6,"z":7,"y":-9},
        |{"count":4,"x":8,"z":9,"y":-12},
        |{"count":5,"x":10,"z":11,"y":-15},
        |{"count":6,"x":12,"z":13,"y":-18},
        |{"count":7,"x":14,"z":15,"y":-21},
        |{"count":8,"x":16,"z":17,"y":-24},
        |{"count":9,"x":18,"z":19,"y":-27},
        |{"count":10,"x":20,"z":21,"y":-30}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 31.2. Command with backspaces") {
    val actual = execute("""streamstats count | eval x=count*2 | eval z=x+1 | eval y = (-1) * count*3 | where ((x > 2 * ( 1 + 1  * (6 - (2+3)) ) and y < 6* (1 + (8-(3+2*2)))) or (  (x=2) and (z=3)  ) ) | fields count, x, z, y""")
    val expected =
      """[
        |{"count":1,"x":2,"z":3,"y":-3},
        |{"count":3,"x":6,"z":7,"y":-9},
        |{"count":4,"x":8,"z":9,"y":-12},
        |{"count":5,"x":10,"z":11,"y":-15},
        |{"count":6,"x":12,"z":13,"y":-18},
        |{"count":7,"x":14,"z":15,"y":-21},
        |{"count":8,"x":16,"z":17,"y":-24},
        |{"count":9,"x":18,"z":19,"y":-27},
        |{"count":10,"x":20,"z":21,"y":-30}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 32. Command: | where <cyrillic_field>") {
    val actual = execute("""eval коэфф = if(serialField<7, 3, 1) | where 'коэфф' = 3""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","serialField":"0","коэфф":3},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField":"1","коэфф":3},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField":"2","коэфф":3},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField":"3","коэфф":3},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField":"4","коэфф":3},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField":"5","коэфф":3},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField":"6","коэфф":3}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 33. Command: |  where not field = value") {
    val actual = execute("""table _raw, _time, serialField | where NOT serialField = 7""")//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0"},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"serialField":"1"},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"serialField":"2"},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"serialField":"3"},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"serialField":"4"},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5"},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6"},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8"},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 34. Command: |  where not field >= value") {
    val actual = execute("""table _raw, _time, random_Field | where not   random_Field >= 50""")//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"random_Field":"-90"},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"random_Field":"20"},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"random_Field":"30"},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"random_Field":"-100"},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"random_Field":"0"},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"random_Field":"10"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 35. Command: |  where not field < value") {
    val actual = execute("""table _raw, _time, serialField | where  Not serialField < 9""")//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 36. Command: |  where not field = text_value") {
    val actual = execute("""table _raw, _time, junkField | where not junkField = "123_ASD" """)//TODO
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"junkField":"q2W"},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"junkField":"132_."},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"junkField":"asd.cx"},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"junkField":"XYZ"},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"junkField":"casd(@#)asd"},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"junkField":"QQQ.2"},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"junkField":"00_3"},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"junkField":"112"},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"junkField":"word"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 37. Command: | where ((.. or ..) and (not .. and ..))") {
    val actual = execute("""where ((random_Field > 70 or random_Field = 30) and (not serialField < 4 and serialField <=9) )""")
    val expected =
      """[
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","random_Field":"30","serialField":"4"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 38. Command: | where ((.. or ..) and not (.. and ..))") {
    val actual = execute("""where ((random_Field > 70 or random_Field = 30) and not (serialField < 4 and serialField <=9) )""")
    val expected =
      """[
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","random_Field":"30","serialField":"4"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 39. Command: | where ((not((.. or ..) or ..) or ..) .. and)") {
    val actual = execute("""where ((not((serialField = 2 or serialField = 7) or serialField = 4) or serialField = 6) and random_Field <= 60)""")
    val expected =
      """[
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField":"1","random_Field":"-90"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField":"3","random_Field":"20"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField":"5","random_Field":"50"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField":"6","random_Field":"60"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField":"8","random_Field":"0"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField":"9","random_Field":"10"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
