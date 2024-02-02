package com.isgneuro.otl.commands

class TableCommandTest extends Command2Test {
  override val dataset: String = """[
  {"_time":1570007900,"_raw":"{\"_time\":1570007900, \"col1\": \"value\", \"Колонка\": \"1\", \"Колонка2\" : \"2\"}","col1": "value", "Колонка": "1",
  "Колонка2" : "2"}]"""

  test("Test 0. Command: | table _time, _raw | Simple table command ") {
    val otlCommand =
      """
        |{
        |        "name": "table",
        |        "arguments": {
        |          "appliedFields": [
        |            {
        |                "key": "",
        |                "type": "term",
        |                "value": "_time",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              },
        |              {
        |                "key": "",
        |                "type": "term",
        |                "value": "_raw",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      }
        |""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected = """[
                      |{"_time":1570007900,"_raw":"{\"_time\":1570007900, \"col1\": \"value\", \"Колонка\": \"1\", \"Колонка2\" : \"2\"}"}
                      |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  /*test("Test 1. Command: | table _time, _raw, Колонка | Simple table command with cyrillic name in column") {
    val otlCommand =
      """
        |{
        |        "name": "table",
        |        "arguments": {
        |          "appliedFields": [
        |            {
        |                "key": "",
        |                "type": "term",
        |                "value": "_time",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              },
        |              {
        |                "key": "",
        |                "type": "term",
        |                "value": "_raw",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              },
        |              {
        |                "key": "",
        |                "type": "term",
        |                "value": "Колонка",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      }
        |""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected = """[
                     |{"_time":1570007900,"_raw":"{\"_time\":1570007900, \"col1\": \"value\", \"Колонка\": \"1\", \"Колонка2\" : \"2\"}","Колонка":"1"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("""Test 2. Command: | rename Колонка as "Колонка с пробелами" | table _time, _raw, "Колонка с пробелами" | Simple table command with cyrillic name and spaces in column""") {
    val otlCommand =
      """{
        |    "name": "rename",
        |    "arguments": {
        |      "field": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "Колонка",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": "Колонка с пробелами"
        |        }
        |      ]
        |    }
        |  },
        |{
        |        "name": "table",
        |        "arguments": {
        |          "appliedFields": [
        |            {
        |                "key": "",
        |                "type": "term",
        |                "value": "_time",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              },
        |              {
        |                "key": "",
        |                "type": "term",
        |                "value": "_raw",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              },
        |              {
        |                "key": "",
        |                "type": "term",
        |                "value": "Колонка с пробелами",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      }
        |""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected = """[
                     |{"_time":1570007900,"_raw":"{\"_time\":1570007900, \"col1\": \"value\", \"Колонка\": \"1\", \"Колонка2\" : \"2\"}","Колонка с пробелами":"1"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("""Test 3. Command: | rename Колонка as "Колонка с пробелами", Колонка2 as "Колонка с пробелами 2" | table
 "Колонка с пробелами", "Колонка с пробелами 2"  """) {
    val otlCommand =
      """{
        |    "name": "rename",
        |    "arguments": {
        |      "field": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "Колонка",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": "Колонка с пробелами"
        |        },
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "Колонка2",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": "Колонка с пробелами 2"
        |        }
        |      ]
        |    }
        |  },
        |{
        |        "name": "table",
        |        "arguments": {
        |          "appliedFields": [
        |              {
        |                "key": "",
        |                "type": "term",
        |                "value": "Колонка с пробелами",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              },
        |              {
        |                "key": "",
        |                "type": "term",
        |                "value": "Колонка с пробелами 2",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      }
        |""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected = """[
    { "Колонка с пробелами": "1","Колонка с пробелами 2": "2" }
    ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4. Command: | table non-existing | ") {
    val otlCommand =
      """
        |{
        |        "name": "table",
        |        "arguments": {
        |          "appliedFields": [
        |            {
        |                "key": "",
        |                "type": "term",
        |                "value": "non-existing",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      }
        |""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected = """[
    {}
    ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Command | table *") {
    val otlCommand =
      """
        |{
        |        "name": "table",
        |        "arguments": {
        |          "appliedFields": [
        |            {
        |                "key": "",
        |                "type": "term",
        |                "value": "*",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      }
        |""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected = """[
                     |{"_raw":"{\"_time\":1570007900, \"col1\": \"value\", \"Колонка\": \"1\", \"Колонка2\" : \"2\"}","_time":1570007900,"col1":"value","Колонка2":"2","Колонка":"1"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 6. Command | table Кол*") {
    val otlCommand =
      """
        |{
        |        "name": "table",
        |        "arguments": {
        |          "appliedFields": [
        |            {
        |                "key": "",
        |                "type": "term",
        |                "value": "Кол*",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      }
        |""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected = """[
     {"Колонка2":"2","Колонка":"1"}
     ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 7. Command | table *олон*") {
    val otlCommand =
      """
        |{
        |        "name": "table",
        |        "arguments": {
        |          "appliedFields": [
        |            {
        |                "key": "",
        |                "type": "term",
        |                "value": "*олон*",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      }
        |""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected = """[
     {"Колонка2":"2","Колонка":"1"}
     ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }*/

}
