package ot.scalaotl.commands

class OTLSortTest extends CommandTest {


  //Не работает сотировка по полю, состоящему из двух слов типа "Это все одно и то же поле".
  ignore("Test 1. Command: | sort  by 'multiple words' field + default order  ") {
    val actual = execute(""" rename junkField as "junk Field"  | sort "junk Field" """)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junk Field\": \"00_3\"}","junk Field":"00_3"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junk Field\": \"112\"}","junk Field":"112"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junk Field\": \"123_ASD\"}","junk Field":"123_ASD"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junk Field\": \"132_.\"}","junk Field":"132_."},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junk Field\": \"QQQ.2\"}","junk Field":"QQQ.2"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junk Field\": \"XYZ\"}","junk Field":"XYZ"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junk Field\": \"asd.cx\"}","junk Field":"asd.cx"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junk Field\": \"casd(@#)asd\"}","junk Field":"casd(@#)asd"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junk Field\": \"q2W\"}","junk Field":"q2W"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junk Field\": \"word\"}","junk Field":"word"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | sort  by field  + implicit default order  + count") {
    val actual = execute("""sort count=3 junkField """)
    val expected =
      """[
          |{"_time":1568026476854,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","junkField":"00_3"},
          |{"_time":1568026476854,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","junkField":"112"},
          |{"_time":1568026476854,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","junkField":"123_ASD"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  //При явном указании возрастающего порядка сортировки сваливаемся с ошибкой
  ignore("Test 3. Command: | sort  by field  + explicit ascending  order  + count ") {
    val actual = execute("""sort count=3 +junkField """)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","junkField":"00_3"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","junkField":"112"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","junkField":"123_ASD"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  //Ожидаю. что при указании знака минус (-) перед полем должна работать убывающая сортировка, а она возрастающая.
  ignore("Test 4. Command: | sort  by field  + explicit descending  order  + count ") {
    val actual = execute("""sort count=3 -serialField """)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Command: | sort  by multiple fields ") {
    val actual = execute("""sort random_Field WordField  """)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","random_Field":"-100","WordField":"RUS"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","random_Field":"-90","WordField":"rty"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","random_Field":"0","WordField":"MMM"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","random_Field":"10","WordField":"USA"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","random_Field":"100","WordField":"qwe"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","random_Field":"20","WordField":"GreenPeace"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","random_Field":"30","WordField":"fgh"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","random_Field":"50","WordField":"jkl"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","random_Field":"50","WordField":"uio"},
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","random_Field":"60","WordField":"zxc"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}