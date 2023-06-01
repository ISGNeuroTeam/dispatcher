package ot.scalaotl.commands

import ot.scalaotl.static.OtDatetime

class OTLCollectTest extends CommandTest {
  val new_index_name:String = "for_test"

  test("Test 0. Command: | collect") {
    cleanIndexFiles(s"$tmpDir/indexes/$new_index_name")
    val actual = execute(s"""table _time, serialField, random_Field, WordField | collect index=$new_index_name """)
    val expected = f"""[
                      |{"_time":1568026476854,"serialField":"0","random_Field":"100","WordField":"qwe","_raw":"WordField=qwe,random_Field=100,serialField=0,_time=1568026476854"},
                      |{"_time":1568026476855,"serialField":"1","random_Field":"-90","WordField":"rty","_raw":"WordField=rty,random_Field=-90,serialField=1,_time=1568026476855"},
                      |{"_time":1568026476856,"serialField":"2","random_Field":"50","WordField":"uio","_raw":"WordField=uio,random_Field=50,serialField=2,_time=1568026476856"},
                      |{"_time":1568026476857,"serialField":"3","random_Field":"20","WordField":"GreenPeace","_raw":"WordField=GreenPeace,random_Field=20,serialField=3,_time=1568026476857"},
                      |{"_time":1568026476858,"serialField":"4","random_Field":"30","WordField":"fgh","_raw":"WordField=fgh,random_Field=30,serialField=4,_time=1568026476858"},
                      |{"_time":1568026476859,"serialField":"5","random_Field":"50","WordField":"jkl","_raw":"WordField=jkl,random_Field=50,serialField=5,_time=1568026476859"},
                      |{"_time":1568026476860,"serialField":"6","random_Field":"60","WordField":"zxc","_raw":"WordField=zxc,random_Field=60,serialField=6,_time=1568026476860"},
                      |{"_time":1568026476861,"serialField":"7","random_Field":"-100","WordField":"RUS","_raw":"WordField=RUS,random_Field=-100,serialField=7,_time=1568026476861"},
                      |{"_time":1568026476862,"serialField":"8","random_Field":"0","WordField":"MMM","_raw":"WordField=MMM,random_Field=0,serialField=8,_time=1568026476862"},
                      |{"_time":1568026476863,"serialField":"9","random_Field":"10","WordField":"USA","_raw":"WordField=USA,random_Field=10,serialField=9,_time=1568026476863"}
                      |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")

  }

  test("Test 1. Command: | collect source=__source__ sourcetype=__sourcetype__") {
    cleanIndexFiles(s"$tmpDir/indexes/$new_index_name")
    val actual = execute("""table _time, serialField, random_Field, WordField | collect index=for_test source=transactions sourcetype=transactions """)
    val expected = """[
                     |{"_time":1568026476854,"serialField":"0","random_Field":"100","WordField":"qwe","_source":"transactions","_sourcetype":"transactions","_raw":"_sourcetype=transactions,_source=transactions,WordField=qwe,random_Field=100,serialField=0,_time=1568026476854"},
                     |{"_time":1568026476855,"serialField":"1","random_Field":"-90","WordField":"rty","_source":"transactions","_sourcetype":"transactions","_raw":"_sourcetype=transactions,_source=transactions,WordField=rty,random_Field=-90,serialField=1,_time=1568026476855"},
                     |{"_time":1568026476856,"serialField":"2","random_Field":"50","WordField":"uio","_source":"transactions","_sourcetype":"transactions","_raw":"_sourcetype=transactions,_source=transactions,WordField=uio,random_Field=50,serialField=2,_time=1568026476856"},
                     |{"_time":1568026476857,"serialField":"3","random_Field":"20","WordField":"GreenPeace","_source":"transactions","_sourcetype":"transactions","_raw":"_sourcetype=transactions,_source=transactions,WordField=GreenPeace,random_Field=20,serialField=3,_time=1568026476857"},
                     |{"_time":1568026476858,"serialField":"4","random_Field":"30","WordField":"fgh","_source":"transactions","_sourcetype":"transactions","_raw":"_sourcetype=transactions,_source=transactions,WordField=fgh,random_Field=30,serialField=4,_time=1568026476858"},
                     |{"_time":1568026476859,"serialField":"5","random_Field":"50","WordField":"jkl","_source":"transactions","_sourcetype":"transactions","_raw":"_sourcetype=transactions,_source=transactions,WordField=jkl,random_Field=50,serialField=5,_time=1568026476859"},
                     |{"_time":1568026476860,"serialField":"6","random_Field":"60","WordField":"zxc","_source":"transactions","_sourcetype":"transactions","_raw":"_sourcetype=transactions,_source=transactions,WordField=zxc,random_Field=60,serialField=6,_time=1568026476860"},
                     |{"_time":1568026476861,"serialField":"7","random_Field":"-100","WordField":"RUS","_source":"transactions","_sourcetype":"transactions","_raw":"_sourcetype=transactions,_source=transactions,WordField=RUS,random_Field=-100,serialField=7,_time=1568026476861"},
                     |{"_time":1568026476862,"serialField":"8","random_Field":"0","WordField":"MMM","_source":"transactions","_sourcetype":"transactions","_raw":"_sourcetype=transactions,_source=transactions,WordField=MMM,random_Field=0,serialField=8,_time=1568026476862"},
                     |{"_time":1568026476863,"serialField":"9","random_Field":"10","WordField":"USA","_source":"transactions","_sourcetype":"transactions","_raw":"_sourcetype=transactions,_source=transactions,WordField=USA,random_Field=10,serialField=9,_time=1568026476863"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")

  }

  test("Test 2. Command: | collect mode=as-is") {
    cleanIndexFiles(s"$tmpDir/indexes/$new_index_name")
    val actual = execute(s"""table _time, serialField, random_Field, WordField | collect index=$new_index_name mode=as-is """)
    val expected = """[
                     |{"_time":1568026476854,"serialField":"0","random_Field":"100","WordField":"qwe"},
                     |{"_time":1568026476855,"serialField":"1","random_Field":"-90","WordField":"rty"},
                     |{"_time":1568026476856,"serialField":"2","random_Field":"50","WordField":"uio"},
                     |{"_time":1568026476857,"serialField":"3","random_Field":"20","WordField":"GreenPeace"},
                     |{"_time":1568026476858,"serialField":"4","random_Field":"30","WordField":"fgh"},
                     |{"_time":1568026476859,"serialField":"5","random_Field":"50","WordField":"jkl"},
                     |{"_time":1568026476860,"serialField":"6","random_Field":"60","WordField":"zxc"},
                     |{"_time":1568026476861,"serialField":"7","random_Field":"-100","WordField":"RUS"},
                     |{"_time":1568026476862,"serialField":"8","random_Field":"0","WordField":"MMM"},
                     |{"_time":1568026476863,"serialField":"9","random_Field":"10","WordField":"USA"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")

  }

  // TODO Fix time problem.
  ignore("Test 3. Command: | collect [wo _time]") {
    cleanIndexFiles(s"$tmpDir/indexes/$new_index_name")
    val actual = execute(s"""table serialField, random_Field, WordField | collect index=$new_index_name """)
    val nowTime = OtDatetime.getCurrentTimeInSeconds() - 1
    val expected = f"""[
                      |{"serialField":"0","random_Field":"100","WordField":"qwe","_time":$nowTime,"_raw":"_time=$nowTime,WordField=qwe,random_Field=100,serialField=0"},
                      |{"serialField":"1","random_Field":"-90","WordField":"rty","_time":$nowTime,"_raw":"_time=$nowTime,WordField=rty,random_Field=-90,serialField=1"},
                      |{"serialField":"2","random_Field":"50","WordField":"uio","_time":$nowTime,"_raw":"_time=$nowTime,WordField=uio,random_Field=50,serialField=2"},
                      |{"serialField":"3","random_Field":"20","WordField":"GreenPeace","_time":$nowTime,"_raw":"_time=$nowTime,WordField=GreenPeace,random_Field=20,serialField=3"},
                      |{"serialField":"4","random_Field":"30","WordField":"fgh","_time":$nowTime,"_raw":"_time=$nowTime,WordField=fgh,random_Field=30,serialField=4"},
                      |{"serialField":"5","random_Field":"50","WordField":"jkl","_time":$nowTime,"_raw":"_time=$nowTime,WordField=jkl,random_Field=50,serialField=5"},
                      |{"serialField":"6","random_Field":"60","WordField":"zxc","_time":$nowTime,"_raw":"_time=$nowTime,WordField=zxc,random_Field=60,serialField=6"},
                      |{"serialField":"7","random_Field":"-100","WordField":"RUS","_time":$nowTime,"_raw":"_time=$nowTime,WordField=RUS,random_Field=-100,serialField=7"},
                      |{"serialField":"8","random_Field":"0","WordField":"MMM","_time":$nowTime,"_raw":"_time=$nowTime,WordField=MMM,random_Field=0,serialField=8"},
                      |{"serialField":"9","random_Field":"10","WordField":"USA","_time":$nowTime,"_raw":"_time=$nowTime,WordField=USA,random_Field=10,serialField=9"}
                      |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


}