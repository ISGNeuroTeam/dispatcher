package ot.scalaotl.commands

class OTLReplaceTest extends CommandTest {

  test("Test 0. Command: | replace") {
    val actual = execute("""eval 'j.field' = junkField | replace word with ZZZ in j.field """)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","junkField":"q2W","j.field":"q2W"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","junkField":"132_.","j.field":"132_."},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","junkField":"asd.cx","j.field":"asd.cx"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","junkField":"XYZ","j.field":"XYZ"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","junkField":"123_ASD","j.field":"123_ASD"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","junkField":"casd(@#)asd","j.field":"casd(@#)asd"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","junkField":"QQQ.2","j.field":"QQQ.2"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","junkField":"00_3","j.field":"00_3"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","junkField":"112","j.field":"112"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","junkField":"word","j.field":"ZZZ"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | replace \" \" with \"\" in text") {
    val actual = execute("""makeresults | eval text = "cat cat" | replace " " with "" in text | fields - _time """)
    val expected =
      """[
        |{"text":"catcat"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | replace \" \" with \" 2 \" in text") {
    val actual = execute("""makeresults | eval text = "1 3" | replace " " with " 2 " in text | fields - _time """)
    val expected =
      """[
        |{"text":"1 2 3"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | replace from field with null value") {

    val actual =  execute(
      """makeresults count=2
         | streamstats count
         | eval text = case(count=1, "a cat", count=2, null)
         | eval text_new = replace(text, " ", "")
         | fields count, text, text_new""")

    val expected = """[
                     |{"count":1,"text":"a cat","text_new":"acat"},
                     |{"count":2}
                     |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4. Command: | replace text with te=xt") {
    val actual = execute("""eval eqField = "defgh" | replace "defgh" with "d=efgh" in eqField """)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","eqField":"d=efgh"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","eqField":"d=efgh"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","eqField":"d=efgh"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","eqField":"d=efgh"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","eqField":"d=efgh"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","eqField":"d=efgh"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","eqField":"d=efgh"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","eqField":"d=efgh"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","eqField":"d=efgh"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","eqField":"d=efgh"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Command: | replace word1 (word2 word3) with simple_word in text") {
    val actual = execute("""makeresults | eval bracesText = "start finish (a*bc)" | replace "start finish \(a*bc\)" with "c_action" in bracesText | fields - _time """)
    val expected =
      """[
        |{"bracesText":"c_action"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
