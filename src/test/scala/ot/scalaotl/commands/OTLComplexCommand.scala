package ot.scalaotl.commands

class OTLComplexCommand extends CommandTest {

  /** sbt -Dmaster=spark://localhost:7077 "testOnly ot.scalaotl.commands.OTLComplexCommand" */
  ignore("Test 0. Command: Complex command") {
//  test("Test 0. Command: Complex command") {
    val actual = execute(""" | inputlookup ds_common.csv | eval result=tonumber(serialField) """)
    //    val expected = """[{
    //                     |  "_time": 1568026476854,
    //                     |  "_raw": "{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}"
    //                     |}]""".stripMargin
    //    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")

    println(actual)
  }
}