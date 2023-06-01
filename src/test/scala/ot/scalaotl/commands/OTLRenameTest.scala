package ot.scalaotl.commands

class OTLRenameTest extends CommandTest {

  test("Test 0. Command: | rename serialField as sf") {
    val actual = execute("""rename serialField as sf""")
    val expected = """[{
                     |  "_time": 1568026476854,
                     |  "_raw": "{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}",
                     |  "sf": "0"
                     |},
                     |{
                     |  "_time": 1568026476855,
                     |  "_raw": "{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}",
                     |  "sf": "1"
                     |},
                     |{
                     |  "_time": 1568026476856,
                     |  "_raw": "{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}",
                     |  "sf": "2"
                     |},
                     |{
                     |  "_time": 1568026476857,
                     |  "_raw": "{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}",
                     |  "sf": "3"
                     |},
                     |{
                     |  "_time": 1568026476858,
                     |  "_raw": "{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}",
                     |  "sf": "4"
                     |},
                     |{
                     |  "_time": 1568026476859,
                     |  "_raw": "{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}",
                     |  "sf": "5"
                     |},
                     |{
                     |  "_time": 1568026476860,
                     |  "_raw": "{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}",
                     |  "sf": "6"
                     |},
                     |{
                     |  "_time": 1568026476861,
                     |  "_raw": "{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}",
                     |  "sf": "7"
                     |},
                     |{
                     |  "_time": 1568026476862,
                     |  "_raw": "{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}",
                     |  "sf": "8"
                     |},
                     |{
                     |  "_time": 1568026476863,
                     |  "_raw": "{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}",
                     |  "sf": "9"
                     |}]""".stripMargin
    assert(jsonCompare(actual, expected), f"Actual:\n$actual\n---\nExpected:\n$expected")
  }

  test("""Test 1. Command: | rename serialField as "sf" """) {
    val actual = execute("""rename serialField as "sf"""")
    val expected =
      """[{
        |  "_time": 1568026476854,
        |  "_raw": "{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}",
        |  "sf": "0"
        |},
        |{
        |  "_time": 1568026476855,
        |  "_raw": "{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}",
        |  "sf": "1"
        |},
        |{
        |  "_time": 1568026476856,
        |  "_raw": "{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}",
        |  "sf": "2"
        |},
        |{
        |  "_time": 1568026476857,
        |  "_raw": "{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}",
        |  "sf": "3"
        |},
        |{
        |  "_time": 1568026476858,
        |  "_raw": "{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}",
        |  "sf": "4"
        |},
        |{
        |  "_time": 1568026476859,
        |  "_raw": "{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}",
        |  "sf": "5"
        |},
        |{
        |  "_time": 1568026476860,
        |  "_raw": "{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}",
        |  "sf": "6"
        |},
        |{
        |  "_time": 1568026476861,
        |  "_raw": "{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}",
        |  "sf": "7"
        |},
        |{
        |  "_time": 1568026476862,
        |  "_raw": "{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}",
        |  "sf": "8"
        |},
        |{
        |  "_time": 1568026476863,
        |  "_raw": "{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}",
        |  "sf": "9"
        |}]""".stripMargin
    assert(jsonCompare(actual, expected), f"Actual:\n$actual\n---\nExpected:\n$expected")
  }

  test("""Test 2. Command: | rename serialField as "sf hk" """) {
    val actual = execute("""rename serialField as "sf hk" """)
    val expected =
      """[{
        |  "_time": 1568026476854,
        |  "_raw": "{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}",
        |  "sf hk": "0"
        |},
        |{
        |  "_time": 1568026476855,
        |  "_raw": "{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}",
        |  "sf hk": "1"
        |},
        |{
        |  "_time": 1568026476856,
        |  "_raw": "{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}",
        |  "sf hk": "2"
        |},
        |{
        |  "_time": 1568026476857,
        |  "_raw": "{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}",
        |  "sf hk": "3"
        |},
        |{
        |  "_time": 1568026476858,
        |  "_raw": "{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}",
        |  "sf hk": "4"
        |},
        |{
        |  "_time": 1568026476859,
        |  "_raw": "{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}",
        |  "sf hk": "5"
        |},
        |{
        |  "_time": 1568026476860,
        |  "_raw": "{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}",
        |  "sf hk": "6"
        |},
        |{
        |  "_time": 1568026476861,
        |  "_raw": "{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}",
        |  "sf hk": "7"
        |},
        |{
        |  "_time": 1568026476862,
        |  "_raw": "{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}",
        |  "sf hk": "8"
        |},
        |{
        |  "_time": 1568026476863,
        |  "_raw": "{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}",
        |  "sf hk": "9"
        |}]""".stripMargin
    assert(jsonCompare(actual, expected), f"Actual:\n$actual\n---\nExpected:\n$expected")
  }


}
