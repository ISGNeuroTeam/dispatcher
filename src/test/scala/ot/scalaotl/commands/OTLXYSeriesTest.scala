package ot.scalaotl.commands

class OTLXYSeriesTest extends CommandTest {

  test("Test 0. Command: | xyseries ") {
    val actual = execute("""xyseries serialField WordField random_Field""")
    val expected = """[
                     |{"serialField":"7","RUS":"-100"},
                     |{"serialField":"3","GreenPeace":"20"},
                     |{"serialField":"8","MMM":"0"},
                     |{"serialField":"0","qwe":"100"},
                     |{"serialField":"5","jkl":"50"},
                     |{"serialField":"6","zxc":"60"},
                     |{"serialField":"9","USA":"10"},
                     |{"serialField":"1","rty":"-90"},
                     |{"serialField":"4","fgh":"30"},
                     |{"serialField":"2","uio":"50"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
