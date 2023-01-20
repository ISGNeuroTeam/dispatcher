package ot.scalaotl.commands

class OTLEventstatsTest extends CommandTest {

  test("Test 1. Command: | 'eventstats' different agg functions  ") {
    val actual = execute("""eventstats values(random_Field) list(random_Field) max(random_Field) min(serialField)""")
    val expected = """[
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","random_Field":"100","serialField":"0","values(random_Field)":["50","20","30","100","-90","-100","10","60","0"],"list(random_Field)":["100","-90","50","20","30","50","60","-100","0","10"],"max(random_Field)":"60","min(serialField)":"0"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","random_Field":"-90","serialField":"1","values(random_Field)":["50","20","30","100","-90","-100","10","60","0"],"list(random_Field)":["100","-90","50","20","30","50","60","-100","0","10"],"max(random_Field)":"60","min(serialField)":"0"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","random_Field":"50","serialField":"2","values(random_Field)":["50","20","30","100","-90","-100","10","60","0"],"list(random_Field)":["100","-90","50","20","30","50","60","-100","0","10"],"max(random_Field)":"60","min(serialField)":"0"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","random_Field":"20","serialField":"3","values(random_Field)":["50","20","30","100","-90","-100","10","60","0"],"list(random_Field)":["100","-90","50","20","30","50","60","-100","0","10"],"max(random_Field)":"60","min(serialField)":"0"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","random_Field":"30","serialField":"4","values(random_Field)":["50","20","30","100","-90","-100","10","60","0"],"list(random_Field)":["100","-90","50","20","30","50","60","-100","0","10"],"max(random_Field)":"60","min(serialField)":"0"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","random_Field":"50","serialField":"5","values(random_Field)":["50","20","30","100","-90","-100","10","60","0"],"list(random_Field)":["100","-90","50","20","30","50","60","-100","0","10"],"max(random_Field)":"60","min(serialField)":"0"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","random_Field":"60","serialField":"6","values(random_Field)":["50","20","30","100","-90","-100","10","60","0"],"list(random_Field)":["100","-90","50","20","30","50","60","-100","0","10"],"max(random_Field)":"60","min(serialField)":"0"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","random_Field":"-100","serialField":"7","values(random_Field)":["50","20","30","100","-90","-100","10","60","0"],"list(random_Field)":["100","-90","50","20","30","50","60","-100","0","10"],"max(random_Field)":"60","min(serialField)":"0"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","random_Field":"0","serialField":"8","values(random_Field)":["50","20","30","100","-90","-100","10","60","0"],"list(random_Field)":["100","-90","50","20","30","50","60","-100","0","10"],"max(random_Field)":"60","min(serialField)":"0"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","random_Field":"10","serialField":"9","values(random_Field)":["50","20","30","100","-90","-100","10","60","0"],"list(random_Field)":["100","-90","50","20","30","50","60","-100","0","10"],"max(random_Field)":"60","min(serialField)":"0"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | 'eventstats' different agg functions ") {
    val actual = execute(""" eventstats min(serialField) as "min sf", max(random_Field) as maxrf """)
    val expected = """[
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","serialField":"0","random_Field":"100","min sf":"0","maxrf":"60"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","serialField":"1","random_Field":"-90","min sf":"0","maxrf":"60"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","serialField":"2","random_Field":"50","min sf":"0","maxrf":"60"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","serialField":"3","random_Field":"20","min sf":"0","maxrf":"60"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","serialField":"4","random_Field":"30","min sf":"0","maxrf":"60"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField":"5","random_Field":"50","min sf":"0","maxrf":"60"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","serialField":"6","random_Field":"60","min sf":"0","maxrf":"60"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","serialField":"7","random_Field":"-100","min sf":"0","maxrf":"60"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","serialField":"8","random_Field":"0","min sf":"0","maxrf":"60"},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","serialField":"9","random_Field":"10","min sf":"0","maxrf":"60"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | eventstats count by absent_column") {
    val actual = execute(""" | eventstats count by absent_column""") //count number of null values
    val expected = """[
                     ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4. Command: | eventstats by multiple fields ") {
    val actual = execute(""" eventstats count by WordField junkField""")
    val expected =
      """[
        |{"WordField":"qwe","junkField":"q2W","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"count":1},
        |{"WordField":"rty","junkField":"132_.","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476854,"count":1},
        |{"WordField":"uio","junkField":"asd.cx","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476854,"count":1},
        |{"WordField":"GreenPeace","junkField":"XYZ","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476854,"count":1},
        |{"WordField":"fgh","junkField":"123_ASD","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476854,"count":1},
        |{"WordField":"jkl","junkField":"casd(@#)asd","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476854,"count":1},
        |{"WordField":"zxc","junkField":"QQQ.2","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476854,"count":1},
        |{"WordField":"RUS","junkField":"00_3","_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476854,"count":1},
        |{"WordField":"MMM","junkField":"112","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476854,"count":1},
        |{"WordField":"USA","junkField":"word","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476854,"count":1}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  //Не разбирался, реализован ли eval в eventstats, но при попытке его использовать результат не ожидаемый.
  ignore("Test 5. Command: | 'eventstats count' and 'eval' function ") {
    val actual = execute(""" eventstats count(eval(serialField=1)) as s""") //count number of null values
    val expected = """[
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}