package ot.scalaotl.commands

class OTLChartTest extends CommandTest {

  test("Test 1. Command: | chart. Multiple words in 'as' statement ") {
    val actual = execute("""chart count as "two words" """)
    val expected = """[
                     |{"two words":10}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | chart. Multiple aggregate functions  + 'by' ") {

    val actual = execute("""eval random_Field=tonumber(random_Field) | chart count as "two words" list(WordField) as list_WordField max(random_Field) as max_random_Field by sourcetype """, jsonToDf(dataset))
    val expected = """[
                     |{"two words":10,"list_WordField":["qwe","rty","uio","GreenPeace","fgh","jkl","zxc","RUS","MMM","USA"],"max_random_Field":100.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  ignore("Test 3. Command: | chart. 'By' plus 'over' test ") {

    val actual = execute("""chart count over source by host""", jsonToDf(dataset))
    val expected = """[
                     |{"random_Field":"-90","1":1},
                     |{"random_Field":"30","4":1},
                     |{"random_Field":"0","8":1},
                     |{"random_Field":"100","0":1},
                     |{"random_Field":"60","6":1},
                     |{"random_Field":"-100","7":1},
                     |{"random_Field":"20","3":1},
                     |{"random_Field":"10","9":1},
                     |{"random_Field":"50","2":1,"5":1}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
