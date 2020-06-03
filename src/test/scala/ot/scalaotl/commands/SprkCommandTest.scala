package ot.scalaotl.commands

class SprkCommandTest extends CommandTest {

  test("Test 0. Command: | command element_at") {
    val actual = execute("""stats values(WordField) as wf| command raw="element_at(wf,1)" """)
    val expected = """[
                     |{"wf":["zxc","MMM","GreenPeace","uio","rty","RUS","USA","qwe","jkl","fgh"],"raw":"zxc"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | command element_at empty array") {
    val actual = execute(""" stats values(x) as wf | command raw="element_at(wf,1)" """)
    val expected = """[
                     |{"wf":[]}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | command element_at") {
    val actual = execute(""" stats values(serialField) as wf| command raw="element_at(wf,1)"| eval x=tonumber(raw) |eval y=mvindex(wf,x)  """)
    val expected = """[
                     |  {"wf":["3","1","2","5","8","4","9","7","6","0"],"raw":"3","x":3.0,"y":["5"]}
                     |  ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
