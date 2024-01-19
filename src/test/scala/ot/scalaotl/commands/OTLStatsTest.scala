package ot.scalaotl.commands

class OTLStatsTest extends CommandTest {

  test("Test 0. Command: | stats ") {
    val actual = execute("""stats values(random_Field) list(random_Field) max(random_Field) min(serialField)""")
    val expected = """[
                     |{"values(random_Field)":["50","20","30","100","-90","-100","10","60","0"],
                     | "list(random_Field)":["100","-90","50","20","30","50","60","-100","0","10"],
                     | "max(random_Field)":"60",
                     | "min(serialField)":"0"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | stats. Multiple words in 'as' statement") {
    val actual = execute(""" stats min(serialField) as "min sf", max(random_Field) as maxrf """)
    val expected = """[
                     |{"min sf":"0", "maxrf":"60"}
    ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | stats count by absent_column") {
    val actual = execute(""" stats count by absent_column""") //count number of null values
    val expected = """[
                     |{"count":10}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3.0. Command: | stats count by column") {
    val actual = execute(""" stats count by random_Field""")
    val expected =
      """[
        |{"random_Field":"-90","count":1},
        |{"random_Field":"30","count":1},
        |{"random_Field":"0","count":1},
        |{"random_Field":"100","count":1},
        |{"random_Field":"60","count":1},
        |{"random_Field":"-100","count":1},
        |{"random_Field":"20","count":1},
        |{"random_Field":"10","count":1},
        |{"random_Field":"50","count":2}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3.1. Command: | stats count by column with alias") {
    val actual = execute(""" stats count as c by random_Field""")
    val expected =
      """[
        |{"random_Field":"-90","c":1},
        |{"random_Field":"30","c":1},
        |{"random_Field":"0","c":1},
        |{"random_Field":"100","c":1},
        |{"random_Field":"60","c":1},
        |{"random_Field":"-100","c":1},
        |{"random_Field":"20","c":1},
        |{"random_Field":"10","c":1},
        |{"random_Field":"50","c":2}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3.2. Command: | stats count by column with alias with backspaces between stats func, as and alias") {
    val actual = execute(""" stats count   as  c by random_Field""")
    val expected =
      """[
        |{"random_Field":"-90","c":1},
        |{"random_Field":"30","c":1},
        |{"random_Field":"0","c":1},
        |{"random_Field":"100","c":1},
        |{"random_Field":"60","c":1},
        |{"random_Field":"-100","c":1},
        |{"random_Field":"20","c":1},
        |{"random_Field":"10","c":1},
        |{"random_Field":"50","c":2}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 4. Command: | stats count as c by absent_column | eval x=c") {
    val actual = execute(""" stats count as c by absent_column | eval x=c""") //count number of null values
    val expected = """[
                     |{"c":10,"x":10}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Command: | stats ambigous column") {
    val actual = execute("""| eval _time_prev=1 | stats first(*) as *_prev, last(*) as * | eval x=_time_prev""")
    val expected = """[
                     |{"WordField_prev":"qwe","_raw_prev":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time_prev_prev":1,"_time_prev":1568026476854,"junkField_prev":"q2W","random_Field_prev":"100","serialField_prev":"0","WordField":"USA","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"junkField":"word","random_Field":"10","serialField":"9","x":1568026476854}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 6. Command: | stats latest(column)") {
    val actual = execute("""stats latest(random_Field)""")
    val expected =
      """[
        |{"latest(random_Field)":"10"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 7. Command: | stats <time-and-non-time-functions> (even number of blocks)") {
    val actual = execute(
      """stats avg(random_Field) sum(serialField) latest(random_Field) earliest(serialField) min(random_Field) max(serialField) """ +
        """latest(junkField) earliest(WordField)""")
    val expected =
      """[
         |{"avg(random_Field)":13.0,"sum(serialField)":45.0,"latest(random_Field)":"10","earliest(serialField)":"0","min(random_Field)":"-100","max(serialField)":"9","latest(junkField)":"word","earliest(WordField)":"qwe"}
      |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 8. Command: | stats <time-and-non-time-functions> (odd number of blocks)") {
    val actual = execute(
      """stats latest(random_Field) earliest(serialField) avg(random_Field) sum(serialField) latest(junkField) earliest(WordField)""")
    val expected =
      """[
        |{"latest(random_Field)":"10","earliest(serialField)":"0","avg(random_Field)":13.0,"sum(serialField)":45.0,"latest(junkField)":"word","earliest(WordField)":"qwe"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 9. Command: | stats <time-and-non-time-functions> with by") {
    val actual = execute("""stats sum(serialField) avg(serialField) latest(junkField) earliest(serialField) by random_Field""")
    val expected =
      """[
        |{"random_Field":"-90","sum(serialField)":1.0,"avg(serialField)":1.0,"latest(junkField)":"132_.","earliest(serialField)":"1"},
        |{"random_Field":"30","sum(serialField)":4.0,"avg(serialField)":4.0,"latest(junkField)":"123_ASD","earliest(serialField)":"4"},
        |{"random_Field":"0","sum(serialField)":8.0,"avg(serialField)":8.0,"latest(junkField)":"112","earliest(serialField)":"8"},
        |{"random_Field":"100","sum(serialField)":0.0,"avg(serialField)":0.0,"latest(junkField)":"q2W","earliest(serialField)":"0"},
        |{"random_Field":"60","sum(serialField)":6.0,"avg(serialField)":6.0,"latest(junkField)":"QQQ.2","earliest(serialField)":"6"},
        |{"random_Field":"-100","sum(serialField)":7.0,"avg(serialField)":7.0,"latest(junkField)":"00_3","earliest(serialField)":"7"},
        |{"random_Field":"20","sum(serialField)":3.0,"avg(serialField)":3.0,"latest(junkField)":"XYZ","earliest(serialField)":"3"},
        |{"random_Field":"10","sum(serialField)":9.0,"avg(serialField)":9.0,"latest(junkField)":"word","earliest(serialField)":"9"},
        |{"random_Field":"50","sum(serialField)":7.0,"avg(serialField)":3.5,"latest(junkField)":"casd(@#)asd","earliest(serialField)":"2"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 10. Command: | stats count AS cnt") {
    val actual = execute("""stats count AS cnt""")
    val expected =
      """[
        {"cnt":10}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 11. Command: | stats latest(column) aS <alias>") {
    val actual = execute("""stats latest(junkField) aS junkLatest""")
    val expected =
      """[
        {"junkLatest":"word"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}