package ot.scalaotl.commands

class OTLEvalTest extends CommandTest {

  test("Test 0. Command: | eval mapred") {
    val actual = execute("""eval result="a """)
    val expected = """[
                     |{"WordField":"qwe","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"result":["a","b","c","qwe"]},
                     |{"WordField":"rty","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"result":["a","b","c","rty"]},
                     |{"WordField":"uio","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"result":["a","b","c","uio"]},
                     |{"WordField":"GreenPeace","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"result":["a","b","c","GreenPeace"]},
                     |{"WordField":"fgh","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"result":["a","b","c","fgh"]},
                     |{"WordField":"jkl","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"result":["a","b","c","jkl"]},
                     |{"WordField":"zxc","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"result":["a","b","c","zxc"]},
                     |{"WordField":"RUS","_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"result":["a","b","c","RUS"]},
                     |{"WordField":"MMM","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"result":["a","b","c","MMM"]},
                     |{"WordField":"USA","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"result":["a","b","c","USA"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1.0 Command: | eval mvindex") {//TODO
    val actual = execute("""stats values(serialField) as sf| eval result=mvindex(sf, 3,5)""")
    val expected = """[
                     |{"sf":["3","1","2","5","8","4","9","7","6","0"],"result":["5","8"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
  //| eval tV2=mvindex('namedcounter-module.health.summary.tags{}.tagValues{}.tagValue',2)
  test("Test 1.1 Command: | eval mvindex") {//TODO
    val actual = execute("""table sf.2| eval result=mvindex('sf.2', 3)""")
    val expected = """[
                     |{},
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

  test("Test 1.2 Command: | eval mvindex") {//TODO
    val actual = execute("""stats values(serialField) as sf| eval result=mvindex(sf, 15)""")
    val expected = """[
                     |{"sf":["3","1","2","5","8","4","9","7","6","0"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1.3 Command: | eval mvindex") {//TODO
    val actual = execute("""eval sf="abc"| eval result=mvindex(sf, 1)""")
    val expected = """[
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","sf":"abc"},
                     |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","sf":"abc"},
                     |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","sf":"abc"},
                     |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","sf":"abc"},
                     |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","sf":"abc"},
                     |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","sf":"abc"},
                     |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","sf":"abc"},
                     |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","sf":"abc"},
                     |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","sf":"abc"},
                     |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","sf":"abc"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | eval mvjoin") {//TODO
    val actual = execute("""stats values(serialField) as sf| eval result=mvjoin(sf, "XXX")""")
    val expected = """[
                     |{"sf":["3","1","2","5","8","4","9","7","6","0"],"result":"3XXX1XXX2XXX5XXX8XXX4XXX9XXX7XXX6XXX0"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3.0 Command: |eval mvrange") {
    val actual = execute("""eval result=mvrange(1,10,2) """)
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"result":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"result":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"result":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"result":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"result":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"result":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"result":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"result":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"result":[1.0,3.0,5.0,7.0,9.0]},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"result":[1.0,3.0,5.0,7.0,9.0]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3.1 Command: |eval mvrange") {
    val actual = execute("""eval result=mvrange(1,10) """)
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3.2 Command: |eval mvrange") {
    val actual = execute("""eval x=tonumber(serialField) |eval x=mvrange(1,x,2) """)
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0","x":[]},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"serialField":"1","x":[1.0]},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"serialField":"2","x":[1.0]},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"serialField":"3","x":[1.0,3.0]},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"serialField":"4","x":[1.0,3.0]},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5","x":[1.0,3.0,5.0]},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6","x":[1.0,3.0,5.0]},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7","x":[1.0,3.0,5.0,7.0]},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8","x":[1.0,3.0,5.0,7.0]},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9","x":[1.0,3.0,5.0,7.0,9.0]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4 Command: | eval mvzip") {//TODO
    val actual = execute("""stats values(random_Field) as rf, values(WordField) as wf| eval result=mvzip(rf,wf,"---")""")
    val expected = """[
                     |{"rf":["50","20","30","100","-90","-100","10","60","0"],
                     |"wf":["zxc","MMM","GreenPeace","uio","rty","RUS","USA","qwe","jkl","fgh"],
                     |"result":["50---zxc","20---MMM","30---GreenPeace","100---uio","-90---rty","-100---RUS","10---USA","60---qwe","0---jkl"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Command: | eval mvsort") {//TODO
    val actual = execute(""" stats values(random_Field) as rf| eval result=mvsort(rf)""")
    val expected = """[
                     |{"rf":["50","20","30","100","-90","-100","10","60","0"],"result":["-100","-90","0","10","100","20","30","50","60"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 7. Command: | eval round") {//TODO
    val actual = execute("""eval t=serialField/100 | eval result=round(t,1)""")
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0","t":0.0,"result":0.0},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"serialField":"1","t":0.01,"result":0.0},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"serialField":"2","t":0.02,"result":0.0},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"serialField":"3","t":0.03,"result":0.0},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"serialField":"4","t":0.04,"result":0.0},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5","t":0.05,"result":0.1},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6","t":0.06,"result":0.1},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7","t":0.07,"result":0.1},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8","t":0.08,"result":0.1},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9","t":0.09,"result":0.1}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 8. Command: | eval sha1") {
    val actual = execute("""eval result=sha1(serialField)""")
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0","result":"b6589fc6ab0dc82cf12099d1c2d40ab994e8410c"},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"serialField":"1","result":"356a192b7913b04c54574d18c28d46e6395428ab"},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"serialField":"2","result":"da4b9237bacccdf19c0760cab7aec4a8359010b0"},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"serialField":"3","result":"77de68daecd823babbb58edb1c8e14d7106e83bb"},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"serialField":"4","result":"1b6453892473a467d07372d45eb05abc2031647a"},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5","result":"ac3478d69a3c81fa62e60f5c3696165a4e5e6ac4"},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6","result":"c1dfd96eea8cc2b62785275bca38ac261256e278"},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7","result":"902ba3cda1883801594b6e1b452790cc53948fda"},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8","result":"fe5dbbcea5ce7e2988b8c69bcfdfde8904aabc1f"},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9","result":"ade7c2cf97f75d009975f4d720d1fa6c19f4897"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 9. Command: | eval sin cos tan") {
    val actual = execute("""eval sin=sin(serialField)| eval cos=cos(serialField)| eval tan=tan(serialField) """)
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0","sin":0.0,"cos":1.0,"tan":0.0},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"serialField":"1","sin":0.8414709848078965,"cos":0.5403023058681398,"tan":1.5574077246549023},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"serialField":"2","sin":0.9092974268256817,"cos":-0.4161468365471424,"tan":-2.185039863261519},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"serialField":"3","sin":0.1411200080598672,"cos":-0.9899924966004454,"tan":-0.1425465430742778},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"serialField":"4","sin":-0.7568024953079282,"cos":-0.6536436208636119,"tan":1.1578212823495777},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5","sin":-0.9589242746631385,"cos":0.28366218546322625,"tan":-3.380515006246586},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6","sin":-0.27941549819892586,"cos":0.9601702866503661,"tan":-0.29100619138474915},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7","sin":0.6569865987187891,"cos":0.7539022543433046,"tan":0.8714479827243187},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8","sin":0.9893582466233818,"cos":-0.14550003380861354,"tan":-6.799711455220379},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9","sin":0.4121184852417566,"cos":-0.9111302618846769,"tan":-0.45231565944180985}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 10. Command: | eval split") {
    val actual = execute("""eval result=split(junkField,"_")""")
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"junkField":"q2W","result":["q2W"]},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"junkField":"132_.","result":["132","."]},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"junkField":"asd.cx","result":["asd.cx"]},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"junkField":"XYZ","result":["XYZ"]},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"junkField":"123_ASD","result":["123","ASD"]},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"junkField":"casd(@#)asd","result":["casd(@#)asd"]},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"junkField":"QQQ.2","result":["QQQ.2"]},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"junkField":"00_3","result":["00","3"]},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"junkField":"112","result":["112"]},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"junkField":"word","result":["word"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 11. Command: | eval sqrt") {
    val actual = execute("""eval result=sqrt(serialField)""")
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0","result":0.0},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"serialField":"1","result":1.0},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"serialField":"2","result":1.4142135623730951},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"serialField":"3","result":1.7320508075688772},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"serialField":"4","result":2.0},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5","result":2.23606797749979},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6","result":2.449489742783178},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7","result":2.6457513110645907},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8","result":2.8284271247461903},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9","result":3.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 12. Command: | eval substr") {
    val actual = execute("""eval result=substr(WordField,0,1) """)
    val expected = """[
                     |{"WordField":"qwe","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"result":"q"},
                     |{"WordField":"rty","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"result":"r"},
                     |{"WordField":"uio","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"result":"u"},
                     |{"WordField":"GreenPeace","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"result":"G"},
                     |{"WordField":"fgh","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"result":"f"},
                     |{"WordField":"jkl","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"result":"j"},
                     |{"WordField":"zxc","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"result":"z"},
                     |{"WordField":"RUS","_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"result":"R"},
                     |{"WordField":"MMM","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"result":"M"},
                     |{"WordField":"USA","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"result":"U"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 13. Command: | eval tonumber") {
    val actual = execute("""eval result=tonumber(serialField)""")
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0","result":0.0},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"serialField":"1","result":1.0},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"serialField":"2","result":2.0},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"serialField":"3","result":3.0},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"serialField":"4","result":4.0},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5","result":5.0},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6","result":6.0},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7","result":7.0},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8","result":8.0},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9","result":9.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 14. Command: | eval tostring") {
    val actual = execute("""eval result=tostring(_time) + "asd"""")
    val expected = """[
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","result":"1568026476854asd"},
                     |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","result":"1568026476855asd"},
                     |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","result":"1568026476856asd"},
                     |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","result":"1568026476857asd"},
                     |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","result":"1568026476858asd"},
                     |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","result":"1568026476859asd"},
                     |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","result":"1568026476860asd"},
                     |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","result":"1568026476861asd"},
                     |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","result":"1568026476862asd"},
                     |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","result":"1568026476863asd"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 15.0. Command: | eval true") {
    val actual = execute("""eval result=if(true(),1,0)""")
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"result":1},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"result":1},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"result":1},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"result":1},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"result":1},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"result":1},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"result":1},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"result":1},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"result":1},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"result":1}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 15.1. Command: | eval if equals") {
    val actual = execute("""eval result=if(random_Field=50,1,2)""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","random_Field":"100","result":2},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","random_Field":"-90","result":2},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","random_Field":"50","result":1},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","random_Field":"20","result":2},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","random_Field":"30","result":2},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","random_Field":"50","result":1},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","random_Field":"60","result":2},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","random_Field":"-100","result":2},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","random_Field":"0","result":2},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","random_Field":"10","result":2}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 16.0 Command: | eval case ") {
    val actual = execute(
      """stats values(random_Field) as val max(random_Field) as mR | eval res=case(1=1, mvzip(val,val,"--"), 1=1,1)"""
    )
    val expected = """[
                     |{"val":["50","20","30","100","-90","-100","10","60","0"],"mR":"60","res":["50--50","20--20","30--30","100--100","-90---90","-100---100","10--10","60--60","0--0"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 16.1 Command: | eval case ") {
    val actual = execute(
      """|table junkField | eval res=case(1=1, split(junkField, "_"), 1=1,1)"""
    )
    val expected = """[
                     |{"junkField":"q2W","res":["q2W"]},
                     |{"junkField":"132_.","res":["132","."]},
                     |{"junkField":"asd.cx","res":["asd.cx"]},
                     |{"junkField":"XYZ","res":["XYZ"]},
                     |{"junkField":"123_ASD","res":["123","ASD"]},
                     |{"junkField":"casd(@#)asd","res":["casd(@#)asd"]},
                     |{"junkField":"QQQ.2","res":["QQQ.2"]},
                     |{"junkField":"00_3","res":["00","3"]},
                     |{"junkField":"112","res":["112"]},
                     |{"junkField":"word","res":["word"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 16.2 Command: | eval case ") {
    val actual = execute(
      """|table junkField | eval res=mvindex(case(1=1, split(junkField, "_"), 1=1,1),0)"""
    )
    val expected = """[
                     |{"junkField":"q2W","res":["q2W"]},
                     |{"junkField":"132_.","res":["132"]},
                     |{"junkField":"asd.cx","res":["asd.cx"]},
                     |{"junkField":"XYZ","res":["XYZ"]},
                     |{"junkField":"123_ASD","res":["123"]},
                     |{"junkField":"casd(@#)asd","res":["casd(@#)asd"]},
                     |{"junkField":"QQQ.2","res":["QQQ.2"]},
                     |{"junkField":"00_3","res":["00"]},
                     |{"junkField":"112","res":["112"]},
                     |{"junkField":"word","res":["word"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 16.3 Command: | eval case ") {
    val actual = execute(
      """|table junkField | eval res=mvindex(case(1=1,"qwer\\\"werty",1=1,split(junkField, "_")),0)"""
    )
    val expected = """[
                     |{"junkField":"q2W","res":["qwer\\\"werty"]},
                     |{"junkField":"132_.","res":["qwer\\\"werty"]},
                     |{"junkField":"asd.cx","res":["qwer\\\"werty"]},
                     |{"junkField":"XYZ","res":["qwer\\\"werty"]},
                     |{"junkField":"123_ASD","res":["qwer\\\"werty"]},
                     |{"junkField":"casd(@#)asd","res":["qwer\\\"werty"]},
                     |{"junkField":"QQQ.2","res":["qwer\\\"werty"]},
                     |{"junkField":"00_3","res":["qwer\\\"werty"]},
                     |{"junkField":"112","res":["qwer\\\"werty"]},
                     |{"junkField":"word","res":["qwer\\\"werty"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 17.0 Command: | eval mvcount") {//TODO
    val actual = execute("""stats values(serialField) as sf| eval result=mvcount(sf)""")
    val expected = """[
                     |{"sf":["3","1","2","5","8","4","9","7","6","0"],"result":10}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 17.1 Command: | eval mvcount not mv") {//TODO
    val actual = execute("""eval sf="abc"| eval result=mvcount(abc)""")
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"sf":"abc"},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"sf":"abc"},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"sf":"abc"},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"sf":"abc"},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"sf":"abc"},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"sf":"abc"},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"sf":"abc"},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"sf":"abc"},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"sf":"abc"},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"sf":"abc"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 18.0 Command: | eval f2=strptime(f1, format)") {
    val actual = execute("""eval f1="Mon Oct 21 05:00:00 EDT 2019" | eval f2=strptime(f1, "%a %b %d %H:%M:%S %Z %Y")""")
    val expected =
      """[
        |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 18.1 Command: | eval f2=strptime(mv f1, format)") {
    val actual = execute("""eval f1="Mon Oct 21 05:00:00 EDT 2019" | stats values(f1) as f2 | eval f2 = mvindex(f2,0) | eval f3=strptime(f2, "%a %b %d %H:%M:%S %Z %Y")""")
    val expected =
      """[
        |{"f2":["Mon Oct 21 05:00:00 EDT 2019"],"f3":1571648400}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 18.2 Command: | eval f2=strptime(null f1, format)") {
    val actual = execute("""stats values(f1) as f2  | eval f3=strptime(f2, "%a %b %d %H:%M:%S %Z %Y")""")
    val expected =
      """[
        |{"f2":[]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 18.3 Command: | eval x = _num_ + strptime(...)") {
    val actual = execute("""| eval g = 1 + strptime("2016-01-01", "%Y-%m-%d")""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","g":1451595601},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","g":1451595601},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","g":1451595601},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","g":1451595601},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","g":1451595601},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","g":1451595601},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","g":1451595601},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","g":1451595601},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","g":1451595601},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","g":1451595601}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 19. Command: | eval. 'Max' and 'min' in query should not replaced with 'array_max' and 'array_min'") {
    val actual = execute(""" eval max = 3 | eval min = 2 | eval newf = if(serialField > 5, max, min) | fields min, max, newf """)
    val expected = """[
                     |{"min":2,"max":3,"newf":2},
                     |{"min":2,"max":3,"newf":2},
                     |{"min":2,"max":3,"newf":2},
                     |{"min":2,"max":3,"newf":2},
                     |{"min":2,"max":3,"newf":2},
                     |{"min":2,"max":3,"newf":2},
                     |{"min":2,"max":3,"newf":3},
                     |{"min":2,"max":3,"newf":3},
                     |{"min":2,"max":3,"newf":3},
                     |{"min":2,"max":3,"newf":3}
    ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 20. Command: | eval. Expression with $ in column names should not fail ") {
    val actual = execute("""
                           | eval $newcol$ = if(serialField > 4, 1, 0)
                           | eval ifcol = if($newcol$ > 0, -10, -20)
                           | eval evalcol = $newcol$ - 0.5
                           | where $newcol$ > 0
                           | fields $newcol$, ifcol, evalcol, serialField
    """)
    val expected = """[
                     |{"$newcol$":1, "ifcol": -10, "evalcol":0.5, "serialField":"5"},
                     |{"$newcol$":1, "ifcol": -10, "evalcol":0.5, "serialField":"6"},
                     |{"$newcol$":1, "ifcol": -10, "evalcol":0.5, "serialField":"7"},
                     |{"$newcol$":1, "ifcol": -10, "evalcol":0.5, "serialField":"8"},
                     |{"$newcol$":1, "ifcol": -10, "evalcol":0.5, "serialField":"9"}
    ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
  test("Test 21. Command: | eval \"Колонка с пробелами\" = \"123\"")
  {
    val actual = execute(""" eval "Колонка с пробелами" = "123" """)
    val expected = """[
   {"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","Колонка с пробелами":"123"},
   {"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","Колонка с пробелами":"123"},
   {"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","Колонка с пробелами":"123"},
   {"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","Колонка с пробелами":"123"},
   {"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","Колонка с пробелами":"123"},
   {"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","Колонка с пробелами":"123"},
   {"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","Колонка с пробелами":"123"},
   {"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","Колонка с пробелами":"123"},
   {"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","Колонка с пробелами":"123"},
   {"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","Колонка с пробелами":"123"}
    ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 22. Command: | eval expr1, expr2") {
    val actual = execute(""" eval a=1, b=2""")
    val expected = """[
                     |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"a":1,"b":2},
                     |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"a":1,"b":2},
                     |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"a":1,"b":2},
                     |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"a":1,"b":2},
                     |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"a":1,"b":2},
                     |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"a":1,"b":2},
                     |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"a":1,"b":2},
                     |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"a":1,"b":2},
                     |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"a":1,"b":2},
                     |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"a":1,"b":2}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 23. Command: | eval ambigous field") {
    val actual = execute(""" eval max = 3, min = 2, newf = if(serialField > 5, max, min), res = split(junkField, "_") | fields min, max, newf, res """)
    val expected = """[
                     |{"min":2,"max":3,"newf":2,"res":["q2W"]},
                     |{"min":2,"max":3,"newf":2,"res":["132","."]},
                     |{"min":2,"max":3,"newf":2,"res":["asd.cx"]},
                     |{"min":2,"max":3,"newf":2,"res":["XYZ"]},
                     |{"min":2,"max":3,"newf":2,"res":["123","ASD"]},
                     |{"min":2,"max":3,"newf":2,"res":["casd(@#)asd"]},
                     |{"min":2,"max":3,"newf":3,"res":["QQQ.2"]},
                     |{"min":2,"max":3,"newf":3,"res":["00","3"]},
                     |{"min":2,"max":3,"newf":3,"res":["112"]},
                     |{"min":2,"max":3,"newf":3,"res":["word"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 24. Command: | eval multiple expressions with multivalues") {
    val actual = execute("""  | eventstats avg(random_Field) as av1 | eval t = if(random_Field > av1 , 1, 0)  """)
    val expected = """[
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","random_Field":"100","av1":13.0,"t":1},
                     |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","random_Field":"-90","av1":13.0,"t":0},
                     |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","random_Field":"50","av1":13.0,"t":1},
                     |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","random_Field":"20","av1":13.0,"t":1},
                     |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","random_Field":"30","av1":13.0,"t":1},
                     |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","random_Field":"50","av1":13.0,"t":1},
                     |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","random_Field":"60","av1":13.0,"t":1},
                     |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","random_Field":"-100","av1":13.0,"t":0},
                     |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","random_Field":"0","av1":13.0,"t":0},
                     |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","random_Field":"10","av1":13.0,"t":0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 25. Command: | eval replace with $ in args") {
    val actual = execute("""  |eval t1=replace(WordField, ".+$", "X") """)
    val expected = """[
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","WordField":"qwe","t1":"X"},
                     |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","WordField":"rty","t1":"X"},
                     |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","WordField":"uio","t1":"X"},
                     |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","WordField":"GreenPeace","t1":"X"},
                     |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","WordField":"fgh","t1":"X"},
                     |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","WordField":"jkl","t1":"X"},
                     |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","WordField":"zxc","t1":"X"},
                     |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","WordField":"RUS","t1":"X"},
                     |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","WordField":"MMM","t1":"X"},
                     |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","WordField":"USA","t1":"X"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 26.0 Command: | eval mvfind") {//TODO
    val actual = execute("""stats values(random_Field) as rf| eval result=mvfind(rf, "-.*" )""")
    val expected = """[
                     |{"rf":["50","20","30","100","-90","-100","10","60","0"],"result":4}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 26.1 Command: | eval mvfind") {//TODO
    val actual = execute("""stats values(random_Field) as rf| eval result=mvfind(x, "-.*" )""")
    val expected = """[
                     |{"rf":["50","20","30","100","-90","-100","10","60","0"]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 26.2 Command: | eval mvfind") {//TODO
    val actual = execute("""stats values(random_Field) as rf| eval result=mvfind(rf, x )""")
    val expected = """[
                     |{"rf":["50","20","30","100","-90","-100","10","60","0"],"result":-1}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 27 Command: | eval <cyrillic_name>") {
    val actual = execute("""eval абв = 9""")
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","абв":9},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","абв":9},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","абв":9},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","абв":9},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","абв":9},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","абв":9},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","абв":9},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","абв":9},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","абв":9},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","абв":9}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}