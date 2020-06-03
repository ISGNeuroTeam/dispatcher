package ot.scalaotl.commands

class OTLMakemvTest extends CommandTest {

  test("""Test 0. Command: | table serialField, WordField, junkField | makemv delim="_" junkField """) {
    val actual = execute("""table serialField, WordField, junkField | makemv delim="_" junkField""")
    val expected = """[
    |   {"serialField":"0","WordField":"qwe","junkField":["q2W"]},
|   {"serialField":"1","WordField":"rty","junkField":["132","."]},
|   {"serialField":"2","WordField":"uio","junkField":["asd.cx"]},
|   {"serialField":"3","WordField":"GreenPeace","junkField":["XYZ"]},
|   {"serialField":"4","WordField":"fgh","junkField":["123","ASD"]},
|   {"serialField":"5","WordField":"jkl","junkField":["casd(@#)asd"]},
|   {"serialField":"6","WordField":"zxc","junkField":["QQQ.2"]},
|   {"serialField":"7","WordField":"RUS","junkField":["00","3"]},
|   {"serialField":"8","WordField":"MMM","junkField":["112"]},
|   {"serialField":"9","WordField":"USA","junkField":["word"]}
|   ]  """.stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

 }


