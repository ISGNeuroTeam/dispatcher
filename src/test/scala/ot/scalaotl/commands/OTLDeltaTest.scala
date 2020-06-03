package ot.scalaotl.commands

//BUG2 Если перед командой delta используется сортировка(команда sort), сортировка сбивается.
class OTLDeltaTest extends CommandTest {

  //BUG1 не работает без указания newfield
  ignore("Test 0. Command: | delta <field>") {
    val actual = execute("""eval random_Field=int(random_Field) | table random_Field | delta random_Field """)
    val expected =
      """[
        |{"random_Field":100,"delta(random_Field)":0},
        |{"random_Field":-90,"delta(random_Field)":-190},
        |{"random_Field":50,"delta(random_Field)":140},
        |{"random_Field":20,"delta(random_Field)":-30},
        |{"random_Field":30,"delta(random_Field)":10},
        |{"random_Field":50,"delta(random_Field)":20},
        |{"random_Field":60,"delta(random_Field)":10},
        |{"random_Field":-100,"delta(random_Field)":-160},
        |{"random_Field":0,"delta(random_Field)":100},
        |{"random_Field":10,"delta(random_Field)":10}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")

  }

  test("Test 1. Command: | delta (<field> [AS <newfield>]) ") {
    val actual = execute("""eval random_Field=int(random_Field) | table random_Field | delta random_Field as d """)
    val expected =
      """[
        |{"random_Field":100,"d":0},
        |{"random_Field":-90,"d":-190},
        |{"random_Field":50,"d":140},
        |{"random_Field":20,"d":-30},
        |{"random_Field":30,"d":10},
        |{"random_Field":50,"d":20},
        |{"random_Field":60,"d":10},
        |{"random_Field":-100,"d":-160},
        |{"random_Field":0,"d":100},
        |{"random_Field":10,"d":10}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")

  }

  test("Test 2. Command: | delta with \"p\" - how many results prior to the current result to use ") {
    val actual = execute("""eval random_Field=int(random_Field) | table random_Field | delta p=2 random_Field as d """)
    val expected =
      """[
        |{"random_Field":100,"d":0},
        |{"random_Field":-90,"d":-190},
        |{"random_Field":50,"d":-50},
        |{"random_Field":20,"d":110},
        |{"random_Field":30,"d":-20},
        |{"random_Field":50,"d":30},
        |{"random_Field":60,"d":30},
        |{"random_Field":-100,"d":-150},
        |{"random_Field":0,"d":-60},
        |{"random_Field":10,"d":110}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")

  }

}