package com.isgneuro.otl.commands

import org.apache.spark.sql.functions.col

class FieldsCommandTest extends Command2Test {

  ignore("Test 1. Command: | Selection of existing fields") {
    val otlCommand =
      """{
        |        "name": "fields",
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
        |                "value": "_meta",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              },
        |              {
        |                "key": "",
        |                "type": "term",
        |                "value": "host",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              },
        |              {
        |                "key": "",
        |                "type": "term",
        |                "value": "sourcetype",
        |                "arg_type": "arg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      }
        |""".stripMargin
    val actual = runQuery(otlCommand, "otstats")
    val expected = readIndexDF(test_index)
      .select(col("_time"), col("_meta"), col("host"), col("sourcetype"))
    compareDataFrames(actual, expected)
  }
}