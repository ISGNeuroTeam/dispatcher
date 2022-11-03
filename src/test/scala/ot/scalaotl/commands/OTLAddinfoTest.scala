package ot.scalaotl.commands

import ot.dispatcher.SuperVisor

class OTLAddinfoTest extends CommandTest{

  test ("") {
    val visor = new SuperVisor
  }

  test("Test 0. Command: | addinfo") {
    val actual = execute("""addinfo """)
    val expected = """[
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","info_min_time":0,"info_max_time":0,"info_sid":0},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","info_min_time":0,"info_max_time":0,"info_sid":0},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","info_min_time":0,"info_max_time":0,"info_sid":0},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","info_min_time":0,"info_max_time":0,"info_sid":0},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","info_min_time":0,"info_max_time":0,"info_sid":0},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","info_min_time":0,"info_max_time":0,"info_sid":0},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","info_min_time":0,"info_max_time":0,"info_sid":0},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","info_min_time":0,"info_max_time":0,"info_sid":0},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","info_min_time":0,"info_max_time":0,"info_sid":0},
                     |{"_time":1568026476854,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","info_min_time":0,"info_max_time":0,"info_sid":0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
