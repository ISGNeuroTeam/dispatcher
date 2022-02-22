package ot.scalaotl.commands
import ot.scalaotl.Converter

class OTLAppendTest extends CommandTest{

  test("Test 0. Command: | append") {
    val ssQuery = createQuery("| makeresults | eval serialField=10, random_Field=777 ")
    val cacheDF = new Converter(ssQuery).run
    val cacheMap = Map("id1" -> cacheDF)

    val otlQuery = createQuery("table serialField, random_Field | append subsearch=id1 | table serialField, random_Field ")
    val resultDF = new Converter(otlQuery, cacheMap).run


    val actual = resultDF.toJSON.collect().mkString("[\n", ",\n", "\n]")
    val expected = """[
                     |{"serialField":"0","random_Field":"100"},
                     |{"serialField":"1","random_Field":"-90"},
                     |{"serialField":"2","random_Field":"50"},
                     |{"serialField":"3","random_Field":"20"},
                     |{"serialField":"4","random_Field":"30"},
                     |{"serialField":"5","random_Field":"50"},
                     |{"serialField":"6","random_Field":"60"},
                     |{"serialField":"7","random_Field":"-100"},
                     |{"serialField":"8","random_Field":"0"},
                     |{"serialField":"9","random_Field":"10"},
                     |{"serialField":"10","random_Field":"777"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
