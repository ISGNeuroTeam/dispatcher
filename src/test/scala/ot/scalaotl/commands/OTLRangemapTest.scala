package ot.scalaotl.commands

class OTLRangemapTest extends CommandTest{

  //BUG1 В диапазон не включается последнее значение
  ignore("Test 0. Command: | rangemap") {
    val actual = execute("""eval random_Field=int(random_Field) | table random_Field | rangemap field=random_Field green=0-30 blue=31-60 red=61-100 """)
    val expected = """[
                     |{"random_Field":100,"range":"red"},
                     |{"random_Field":-90,"range":"None"},
                     |{"random_Field":50,"range":"blue"},
                     |{"random_Field":20,"range":"green"},
                     |{"random_Field":30,"range":"green"},
                     |{"random_Field":50,"range":"blue"},
                     |{"random_Field":60,"range":"blue"},
                     |{"random_Field":-100,"range":"None"},
                     |{"random_Field":0,"range":"green"},
                     |{"random_Field":10,"range":"green"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  //BUG2 В диапозон нельзя вклчить отрицательные значения
  ignore("Test 1. Command: | rangemap with minus values") {
    val actual = execute("""eval random_Field=int(random_Field) | table random_Field | rangemap field=random_Field grey=-100-0 green=1-30 blue=31-60 red=61-100 """)
    val expected = """[
                     |{"random_Field":100,"range":"red"},
                     |{"random_Field":-90,"range":"grey"},
                     |{"random_Field":50,"range":"blue"},
                     |{"random_Field":20,"range":"green"},
                     |{"random_Field":30,"range":"green"},
                     |{"random_Field":50,"range":"blue"},
                     |{"random_Field":60,"range":"blue"},
                     |{"random_Field":-100,"range":"grey"},
                     |{"random_Field":0,"range":"grey"},
                     |{"random_Field":10,"range":"green"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | rangemap with default") {
    val actual = execute("""table random_Field | rangemap field=random_Field green=0-40 blue=41-70 red=71-110 default=not_in_range""")
    val expected = """[
                     |{"random_Field":"100","range":"red"},
                     |{"random_Field":"-90","range":"not_in_range"},
                     |{"random_Field":"50","range":"blue"},
                     |{"random_Field":"20","range":"green"},
                     |{"random_Field":"30","range":"green"},
                     |{"random_Field":"50","range":"blue"},
                     |{"random_Field":"60","range":"blue"},
                     |{"random_Field":"-100","range":"not_in_range"},
                     |{"random_Field":"0","range":"green"},
                     |{"random_Field":"10","range":"green"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
