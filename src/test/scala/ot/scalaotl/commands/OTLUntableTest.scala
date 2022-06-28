package ot.scalaotl.commands

import ot.scalaotl.Converter

class OTLUntableTest extends CommandTest {

  test("Test 0. Command: | untable ") {
    val query = createQuery(
      """table WordField ,serialField, random_Field | untable WordField, field, value""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    val expected = """[
                     |{"WordField":"qwe","field":"serialField","value":"0"},
                     |{"WordField":"qwe","field":"random_Field","value":"100"},
                     |{"WordField":"rty","field":"serialField","value":"1"},
                     |{"WordField":"rty","field":"random_Field","value":"-90"},
                     |{"WordField":"uio","field":"serialField","value":"2"},
                     |{"WordField":"uio","field":"random_Field","value":"50"},
                     |{"WordField":"GreenPeace","field":"serialField","value":"3"},
                     |{"WordField":"GreenPeace","field":"random_Field","value":"20"},
                     |{"WordField":"fgh","field":"serialField","value":"4"},
                     |{"WordField":"fgh","field":"random_Field","value":"30"},
                     |{"WordField":"jkl","field":"serialField","value":"5"},
                     |{"WordField":"jkl","field":"random_Field","value":"50"},
                     |{"WordField":"zxc","field":"serialField","value":"6"},
                     |{"WordField":"zxc","field":"random_Field","value":"60"},
                     |{"WordField":"RUS","field":"serialField","value":"7"},
                     |{"WordField":"RUS","field":"random_Field","value":"-100"},
                     |{"WordField":"MMM","field":"serialField","value":"8"},
                     |{"WordField":"MMM","field":"random_Field","value":"0"},
                     |{"WordField":"USA","field":"serialField","value":"9"},
                     |{"WordField":"USA","field":"random_Field","value":"10"}
                     |]""".stripMargin
    compareDataFrames(actual, jsonToDf(expected))
  }

}
