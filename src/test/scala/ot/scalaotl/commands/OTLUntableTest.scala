package ot.scalaotl.commands

class OTLUntableTest extends CommandTest {

  test("Test 0. Command: | untable ") {
    val actual = execute(""" table serialField, random_Field, WordField | untable WordField c1 c2""")
    val expected = """[
                     |{"WordField":"qwe","c1":"serialField","c2":"0"},
                     |{"WordField":"qwe","c1":"random_Field","c2":"100"},
                     |{"WordField":"rty","c1":"serialField","c2":"1"},
                     |{"WordField":"rty","c1":"random_Field","c2":"-90"},
                     |{"WordField":"uio","c1":"serialField","c2":"2"},
                     |{"WordField":"uio","c1":"random_Field","c2":"50"},
                     |{"WordField":"GreenPeace","c1":"serialField","c2":"3"},
                     |{"WordField":"GreenPeace","c1":"random_Field","c2":"20"},
                     |{"WordField":"fgh","c1":"serialField","c2":"4"},
                     |{"WordField":"fgh","c1":"random_Field","c2":"30"},
                     |{"WordField":"jkl","c1":"serialField","c2":"5"},
                     |{"WordField":"jkl","c1":"random_Field","c2":"50"},
                     |{"WordField":"zxc","c1":"serialField","c2":"6"},
                     |{"WordField":"zxc","c1":"random_Field","c2":"60"},
                     |{"WordField":"RUS","c1":"serialField","c2":"7"},
                     |{"WordField":"RUS","c1":"random_Field","c2":"-100"},
                     |{"WordField":"MMM","c1":"serialField","c2":"8"},
                     |{"WordField":"MMM","c1":"random_Field","c2":"0"},
                     |{"WordField":"USA","c1":"serialField","c2":"9"},
                     |{"WordField":"USA","c1":"random_Field","c2":"10"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
