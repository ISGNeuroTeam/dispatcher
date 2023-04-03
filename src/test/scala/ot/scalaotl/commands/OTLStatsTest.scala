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

  test("Test 3. Command: | stats count by column") {
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
                     |{"WordField_prev":"qwe","_raw_prev":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time_prev_prev":1,"_time_prev":1568026476854,"junkField_prev":"q2W","random_Field_prev":"100","serialField_prev":"0","WordField":"USA","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476854,"junkField":"word","random_Field":"10","serialField":"9","x":1568026476854}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}