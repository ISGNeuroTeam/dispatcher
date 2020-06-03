package ot.scalaotl.commands

class OTLCollectTest extends CommandTest {

  //BUG1 Сохраняет данные в неверный формат бакетов
  ignore("Test 0. Command: | collect") {
    val actual = execute("""table serialField, random_Field, WordField | collect index=for_test source=transactions sourcetype=transactions """)
    val expected = """[
                     |{"serialField":"0","random_Field":"100","WordField":"qwe","_time":0,"_%raw":"_time=0,WordField=qwe,random_Field=100,serialField=0"},
                     |{"serialField":"1","random_Field":"-90","WordField":"rty","_time":0,"_%raw":"_time=0,WordField=rty,random_Field=-90,serialField=1"},
                     |{"serialField":"2","random_Field":"50","WordField":"uio","_time":0,"_%raw":"_time=0,WordField=uio,random_Field=50,serialField=2"},
                     |{"serialField":"3","random_Field":"20","WordField":"GreenPeace","_time":0,"_%raw":"_time=0,WordField=GreenPeace,random_Field=20,serialField=3"},
                     |{"serialField":"4","random_Field":"30","WordField":"fgh","_time":0,"_%raw":"_time=0,WordField=fgh,random_Field=30,serialField=4"},
                     |{"serialField":"5","random_Field":"50","WordField":"jkl","_time":0,"_%raw":"_time=0,WordField=jkl,random_Field=50,serialField=5"},
                     |{"serialField":"6","random_Field":"60","WordField":"zxc","_time":0,"_%raw":"_time=0,WordField=zxc,random_Field=60,serialField=6"},
                     |{"serialField":"7","random_Field":"-100","WordField":"RUS","_time":0,"_%raw":"_time=0,WordField=RUS,random_Field=-100,serialField=7"},
                     |{"serialField":"8","random_Field":"0","WordField":"MMM","_time":0,"_%raw":"_time=0,WordField=MMM,random_Field=0,serialField=8"},
                     |{"serialField":"9","random_Field":"10","WordField":"USA","_time":0,"_%raw":"_time=0,WordField=USA,random_Field=10,serialField=9"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
