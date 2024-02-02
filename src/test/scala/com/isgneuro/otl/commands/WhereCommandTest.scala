package com.isgneuro.otl.commands

class WhereCommandTest extends Command2Test {

  ignore("Test 1. Command: |  where field = mvalue") {
    val otlCommand =
      """{
              "name": "where",
              "arguments": {
                "expression": [
                  {
                      "key": "WordField",
                      "type": "string",
                      "value": "jkl",
                      "arg_type": "kwarg",
                      "group_by": [],
                      "named_as": ""
                    }
                ]
              }
            }"""
    val actual = executeQuery(otlCommand) //TODO
    val expected =
      """[
        |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","serialField":"5","_time":1568026476859},
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
