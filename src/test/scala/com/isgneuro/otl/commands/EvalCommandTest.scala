package com.isgneuro.otl.commands

class EvalCommandTest extends Command2Test {
  test("Test 0. Command: | eval mapred") {
    val otlCommand =
      """{
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
                "key": "result",
                "type": "function",
                "value": {
                  "type": "function",
                  "funcargs": [
                    {
                      "type": "string",
                      "value": "a",
                      "leaf_type": "simple",
                      "grouped_by": []
                    },
                    {
                      "type": "string",
                      "value": "b",
                      "leaf_type": "simple",
                      "grouped_by": []
                    },
                    {
                      "type": "string",
                      "value": "c",
                      "leaf_type": "simple",
                      "grouped_by": []
                    }
                  ],
                  "funcname": {
                    "type": "term",
                    "value": "mvappend",
                    "leaf_type": "simple"
                  },
                  "leaf_type": "complex"
                },
                "arg_type": "tree",
                "group_by": [],
                "named_as": ""
              }
          ]
        }
      }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","result":["a","b","c"]},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","result":["a","b","c"]},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","result":["a","b","c"]},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","result":["a","b","c"]},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","result":["a","b","c"]},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","result":["a","b","c"]},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","result":["a","b","c"]},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","result":["a","b","c"]},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","result":["a","b","c"]},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","result":["a","b","c"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1.0 Command: | eval mvindex") { //TODO
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "a",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 10,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "a",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 3,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 5,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvindex",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["4.0","5.0"]},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["4.0","5.0"]},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["4.0","5.0"]},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["4.0","5.0"]},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["4.0","5.0"]},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["4.0","5.0"]},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["4.0","5.0"]},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["4.0","5.0"]},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["4.0","5.0"]},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["4.0","5.0"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
  //| eval tV2=mvindex('namedcounter-module.health.summary.tags{}.tagValues{}.tagValue',2)
  /*test("Test 1.1 Command: | eval mvindex") { //TODO
    val actual = execute("""table sf.2| eval result=mvindex('sf.2', 3)""")
    val expected =
      """[
        |{},
        |{},
        |{},
        |{},
        |{},
        |{},
        |{},
        |{},
        |{},
        |{}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }*/

  test("Test 1.2 Command: | eval mvindex") { //TODO
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "a",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 10,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "a",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 8,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvindex",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["9.0"]},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["9.0"]},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["9.0"]},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["9.0"]},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["9.0"]},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["9.0"]},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["9.0"]},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["9.0"]},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["9.0"]},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","a":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":["9.0"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1.3 Command: | eval mvindex") { //TODO
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "sf",
        |          "type": "string",
        |          "value": "abc",
        |          "arg_type": "kwarg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "sf",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvindex",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","sf":"abc"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","sf":"abc"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","sf":"abc"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","sf":"abc"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","sf":"abc"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","sf":"abc"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","sf":"abc"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","sf":"abc"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","sf":"abc"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","sf":"abc"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | eval mvjoin") { //TODO
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "sf",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 10,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "sf",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "string",
        |                "value": "XXX",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvjoin",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":"1.0XXX2.0XXX3.0XXX4.0XXX5.0XXX6.0XXX7.0XXX8.0XXX9.0XXX10.0"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":"1.0XXX2.0XXX3.0XXX4.0XXX5.0XXX6.0XXX7.0XXX8.0XXX9.0XXX10.0"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":"1.0XXX2.0XXX3.0XXX4.0XXX5.0XXX6.0XXX7.0XXX8.0XXX9.0XXX10.0"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":"1.0XXX2.0XXX3.0XXX4.0XXX5.0XXX6.0XXX7.0XXX8.0XXX9.0XXX10.0"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":"1.0XXX2.0XXX3.0XXX4.0XXX5.0XXX6.0XXX7.0XXX8.0XXX9.0XXX10.0"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":"1.0XXX2.0XXX3.0XXX4.0XXX5.0XXX6.0XXX7.0XXX8.0XXX9.0XXX10.0"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":"1.0XXX2.0XXX3.0XXX4.0XXX5.0XXX6.0XXX7.0XXX8.0XXX9.0XXX10.0"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":"1.0XXX2.0XXX3.0XXX4.0XXX5.0XXX6.0XXX7.0XXX8.0XXX9.0XXX10.0"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":"1.0XXX2.0XXX3.0XXX4.0XXX5.0XXX6.0XXX7.0XXX8.0XXX9.0XXX10.0"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":"1.0XXX2.0XXX3.0XXX4.0XXX5.0XXX6.0XXX7.0XXX8.0XXX9.0XXX10.0"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3.0 Command: |eval mvrange") {
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 10,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 2,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","result":[1.0,3.0,5.0,7.0,9.0]},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","result":[1.0,3.0,5.0,7.0,9.0]},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","result":[1.0,3.0,5.0,7.0,9.0]},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","result":[1.0,3.0,5.0,7.0,9.0]},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","result":[1.0,3.0,5.0,7.0,9.0]},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","result":[1.0,3.0,5.0,7.0,9.0]},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","result":[1.0,3.0,5.0,7.0,9.0]},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","result":[1.0,3.0,5.0,7.0,9.0]},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","result":[1.0,3.0,5.0,7.0,9.0]},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","result":[1.0,3.0,5.0,7.0,9.0]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3.1 Command: |eval mvrange") {
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 10,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","result":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3.2 Command: |eval mvrange") {
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "x",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "serialField",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "tonumber",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "x",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "term",
        |                "value": "x",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 2,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"serialField":"0","x":[]},
        |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"serialField":"1","x":[1.0]},
        |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"serialField":"2","x":[1.0]},
        |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"serialField":"3","x":[1.0,3.0]},
        |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"serialField":"4","x":[1.0,3.0]},
        |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"serialField":"5","x":[1.0,3.0,5.0]},
        |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"serialField":"6","x":[1.0,3.0,5.0]},
        |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"serialField":"7","x":[1.0,3.0,5.0,7.0]},
        |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"serialField":"8","x":[1.0,3.0,5.0,7.0]},
        |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"serialField":"9","x":[1.0,3.0,5.0,7.0,9.0]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4 Command: | eval mvzip") { //TODO
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "rf",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 10,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "wf",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 11,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 20,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "rf",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "term",
        |                "value": "wf",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "string",
        |                "value": "---",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvzip",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"wf":[11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0],"result":["1.0---11.0","2.0---12.0","3.0---13.0","4.0---14.0","5.0---15.0","6.0---16.0","7.0---17.0","8.0---18.0","9.0---19.0","10.0---20.0"]},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"wf":[11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0],"result":["1.0---11.0","2.0---12.0","3.0---13.0","4.0---14.0","5.0---15.0","6.0---16.0","7.0---17.0","8.0---18.0","9.0---19.0","10.0---20.0"]},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"wf":[11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0],"result":["1.0---11.0","2.0---12.0","3.0---13.0","4.0---14.0","5.0---15.0","6.0---16.0","7.0---17.0","8.0---18.0","9.0---19.0","10.0---20.0"]},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"wf":[11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0],"result":["1.0---11.0","2.0---12.0","3.0---13.0","4.0---14.0","5.0---15.0","6.0---16.0","7.0---17.0","8.0---18.0","9.0---19.0","10.0---20.0"]},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"wf":[11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0],"result":["1.0---11.0","2.0---12.0","3.0---13.0","4.0---14.0","5.0---15.0","6.0---16.0","7.0---17.0","8.0---18.0","9.0---19.0","10.0---20.0"]},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"wf":[11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0],"result":["1.0---11.0","2.0---12.0","3.0---13.0","4.0---14.0","5.0---15.0","6.0---16.0","7.0---17.0","8.0---18.0","9.0---19.0","10.0---20.0"]},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"wf":[11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0],"result":["1.0---11.0","2.0---12.0","3.0---13.0","4.0---14.0","5.0---15.0","6.0---16.0","7.0---17.0","8.0---18.0","9.0---19.0","10.0---20.0"]},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"wf":[11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0],"result":["1.0---11.0","2.0---12.0","3.0---13.0","4.0---14.0","5.0---15.0","6.0---16.0","7.0---17.0","8.0---18.0","9.0---19.0","10.0---20.0"]},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"wf":[11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0],"result":["1.0---11.0","2.0---12.0","3.0---13.0","4.0---14.0","5.0---15.0","6.0---16.0","7.0---17.0","8.0---18.0","9.0---19.0","10.0---20.0"]},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"wf":[11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0],"result":["1.0---11.0","2.0---12.0","3.0---13.0","4.0---14.0","5.0---15.0","6.0---16.0","7.0---17.0","8.0---18.0","9.0---19.0","10.0---20.0"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  /*test("Test 5. Command: | eval mvsort") { //TODO
    val actual = execute(""" stats values(random_Field) as rf| eval result=mvsort(rf)""")
    val expected =
      """[
        |{"rf":["50","20","30","100","-90","-100","10","60","0"],"result":["-100","-90","0","10","100","20","30","50","60"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }*/

  test("Test 7. Command: | eval round") { //TODO
    val otlCommand =
      """{
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
                "key": "s",
                "type": "integer",
                "value": 8,
                "arg_type": "kwarg",
                "group_by": [],
                "named_as": ""
            }
          ]
        }
      },
      {
          "name": "eval",
          "arguments": {
            "evaluations": [
              {
                "key": "t",
                "type": "expr",
                "value": {
                    "op": "/",
                    "lhs": {
                      "type": "term",
                      "value": "s",
                      "leaf_type": "simple"
                    },
                    "rhs": {
                      "type": "integer",
                      "value": 100,
                      "leaf_type": "simple"
                    },
                    "type": "expr",
                    "leaf_type": "complex"
                  },
                "arg_type": "tree",
                "group_by": [],
                "named_as": ""
              }
            ]
          }
        },
            {
                "name": "eval",
                "arguments": {
                  "evaluations": [
                    {
                      "key": "result",
                      "type": "function",
                      "value": {
                        "type": "function",
                        "funcargs": [
                          {
                            "type": "term",
                            "value": "t",
                            "leaf_type": "simple",
                            "grouped_by": []
                          },
                          {
                            "type": "integer",
                            "value": 1,
                            "leaf_type": "simple",
                             "grouped_by": []
                          }
                        ],
                        "funcname": {
                          "type": "term",
                          "value": "round",
                          "leaf_type": "simple"
                        },
                        "leaf_type": "complex"
                      },
                      "arg_type": "tree",
                      "group_by": [],
                      "named_as": ""
                    }
                  ]
                }
              }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","s":8,"t":0.08,"result":0.1},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","s":8,"t":0.08,"result":0.1},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","s":8,"t":0.08,"result":0.1},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","s":8,"t":0.08,"result":0.1},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","s":8,"t":0.08,"result":0.1},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","s":8,"t":0.08,"result":0.1},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","s":8,"t":0.08,"result":0.1},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","s":8,"t":0.08,"result":0.1},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","s":8,"t":0.08,"result":0.1},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","s":8,"t":0.08,"result":0.1}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 8. Command: | eval sha1") {
    val otlCommand =
      """{
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
                "key": "a",
                "type": "integer",
                "value": 533,
                "arg_type": "kwarg",
                "group_by": [],
                "named_as": ""
            }
          ]
        }
      },
      {
          "name": "eval",
          "arguments": {
            "evaluations": [
              {
                "key": "result",
                "type": "function",
                "value": {
                  "type": "function",
                  "funcargs": [
                    {
                      "type": "term",
                      "value": "a",
                      "leaf_type": "simple",
                      "grouped_by": []
                    }
                  ],
                  "funcname": {
                    "type": "term",
                    "value": "sha1",
                    "leaf_type": "simple"
                  },
                  "leaf_type": "complex"
                },
                "arg_type": "tree",
                "group_by": [],
                "named_as": ""
              }
            ]
          }
        }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","a":533,"result":"3a69aa1b60febf635d84cdca387928f10062450d"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","a":533,"result":"3a69aa1b60febf635d84cdca387928f10062450d"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","a":533,"result":"3a69aa1b60febf635d84cdca387928f10062450d"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","a":533,"result":"3a69aa1b60febf635d84cdca387928f10062450d"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","a":533,"result":"3a69aa1b60febf635d84cdca387928f10062450d"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","a":533,"result":"3a69aa1b60febf635d84cdca387928f10062450d"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","a":533,"result":"3a69aa1b60febf635d84cdca387928f10062450d"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","a":533,"result":"3a69aa1b60febf635d84cdca387928f10062450d"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","a":533,"result":"3a69aa1b60febf635d84cdca387928f10062450d"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","a":533,"result":"3a69aa1b60febf635d84cdca387928f10062450d"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 9. Command: | eval sin cos tan") {
    val otlCommand =
      """{
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
                "key": "gr",
                "type": "integer",
                "value": 45,
                "arg_type": "kwarg",
                "group_by": [],
                "named_as": ""
            }
          ]
        }
      },
      {
          "name": "eval",
          "arguments": {
            "evaluations": [
              {
                "key": "sin",
                "type": "function",
                "value": {
                  "type": "function",
                  "funcargs": [
                    {
                      "type": "term",
                      "value": "gr",
                      "leaf_type": "simple",
                      "grouped_by": []
                    }
                  ],
                  "funcname": {
                    "type": "term",
                    "value": "sin",
                    "leaf_type": "simple"
                  },
                  "leaf_type": "complex"
                },
                "arg_type": "tree",
                "group_by": [],
                "named_as": ""
              }
            ]
          }
        },
            {
                "name": "eval",
                "arguments": {
                  "evaluations": [
                    {
                      "key": "cos",
                      "type": "function",
                      "value": {
                        "type": "function",
                        "funcargs": [
                          {
                            "type": "term",
                            "value": "gr",
                            "leaf_type": "simple",
                            "grouped_by": []
                          }
                        ],
                        "funcname": {
                          "type": "term",
                          "value": "cos",
                          "leaf_type": "simple"
                        },
                        "leaf_type": "complex"
                      },
                      "arg_type": "tree",
                      "group_by": [],
                      "named_as": ""
                    }
                  ]
                }
              },
            {
                "name": "eval",
                "arguments": {
                  "evaluations": [
                    {
                      "key": "tan",
                      "type": "function",
                      "value": {
                        "type": "function",
                        "funcargs": [
                          {
                            "type": "term",
                            "value": "gr",
                            "leaf_type": "simple",
                            "grouped_by": []
                          }
                        ],
                        "funcname": {
                          "type": "term",
                          "value": "tan",
                          "leaf_type": "simple"
                        },
                        "leaf_type": "complex"
                      },
                      "arg_type": "tree",
                      "group_by": [],
                      "named_as": ""
                    }
                  ]
                }
              }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","gr":45,"sin":0.8509035245341184,"cos":0.5253219888177297,"tan":1.6197751905438615},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","gr":45,"sin":0.8509035245341184,"cos":0.5253219888177297,"tan":1.6197751905438615},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","gr":45,"sin":0.8509035245341184,"cos":0.5253219888177297,"tan":1.6197751905438615},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","gr":45,"sin":0.8509035245341184,"cos":0.5253219888177297,"tan":1.6197751905438615},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","gr":45,"sin":0.8509035245341184,"cos":0.5253219888177297,"tan":1.6197751905438615},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","gr":45,"sin":0.8509035245341184,"cos":0.5253219888177297,"tan":1.6197751905438615},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","gr":45,"sin":0.8509035245341184,"cos":0.5253219888177297,"tan":1.6197751905438615},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","gr":45,"sin":0.8509035245341184,"cos":0.5253219888177297,"tan":1.6197751905438615},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","gr":45,"sin":0.8509035245341184,"cos":0.5253219888177297,"tan":1.6197751905438615},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","gr":45,"sin":0.8509035245341184,"cos":0.5253219888177297,"tan":1.6197751905438615}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 10. Command: | eval split") {
    val otlCommand =
      """{
      "name": "eval",
      "arguments": {
        "evaluations": [
          {
              "key": "symbols",
              "type": "string",
              "value": "mn_rd_kln_az",
              "arg_type": "kwarg",
              "group_by": [],
              "named_as": ""
          }
        ]
      }
    },
    {
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
              "key": "result",
              "type": "function",
              "value": {
                "type": "function",
                "funcargs": [
                  {
                    "type": "term",
                    "value": "symbols",
                    "leaf_type": "simple",
                    "grouped_by": []
                  },
                  {
                          "type": "string",
                          "value": "_",
                          "leaf_type": "simple",
                          "grouped_by": []
                  }
                ],
                "funcname": {
                  "type": "term",
                  "value": "split",
                  "leaf_type": "simple"
                },
                "leaf_type": "complex"
              },
              "arg_type": "tree",
              "group_by": [],
              "named_as": ""
            }
          ]
        }
      }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","symbols":"mn_rd_kln_az","result":["mn","rd","kln","az"]},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","symbols":"mn_rd_kln_az","result":["mn","rd","kln","az"]},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","symbols":"mn_rd_kln_az","result":["mn","rd","kln","az"]},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","symbols":"mn_rd_kln_az","result":["mn","rd","kln","az"]},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","symbols":"mn_rd_kln_az","result":["mn","rd","kln","az"]},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","symbols":"mn_rd_kln_az","result":["mn","rd","kln","az"]},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","symbols":"mn_rd_kln_az","result":["mn","rd","kln","az"]},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","symbols":"mn_rd_kln_az","result":["mn","rd","kln","az"]},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","symbols":"mn_rd_kln_az","result":["mn","rd","kln","az"]},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","symbols":"mn_rd_kln_az","result":["mn","rd","kln","az"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 11. Command: | eval sqrt") {
    val otlCommand =
      """{
      "name": "eval",
      "arguments": {
        "evaluations": [
          {
              "key": "num",
              "type": "integer",
              "value": 64,
              "arg_type": "kwarg",
              "group_by": [],
              "named_as": ""
          }
        ]
      }
    },
    {
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
              "key": "result",
              "type": "function",
              "value": {
                "type": "function",
                "funcargs": [
                  {
                    "type": "term",
                    "value": "num",
                    "leaf_type": "simple",
                    "grouped_by": []
                  }
                ],
                "funcname": {
                  "type": "term",
                  "value": "sqrt",
                  "leaf_type": "simple"
                },
                "leaf_type": "complex"
              },
              "arg_type": "tree",
              "group_by": [],
              "named_as": ""
            }
          ]
        }
      }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","num":64,"result":8.0},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","num":64,"result":8.0},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","num":64,"result":8.0},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","num":64,"result":8.0},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","num":64,"result":8.0},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","num":64,"result":8.0},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","num":64,"result":8.0},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","num":64,"result":8.0},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","num":64,"result":8.0},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","num":64,"result":8.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 12. Command: | eval substr") {
    val otlCommand =
      """{
      "name": "eval",
      "arguments": {
        "evaluations": [
          {
              "key": "abc",
              "type": "string",
              "value": "abcdefgh",
              "arg_type": "kwarg",
              "group_by": [],
              "named_as": ""
          }
        ]
      }
    },
    {
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
              "key": "result",
              "type": "function",
              "value": {
                "type": "function",
                "funcargs": [
                  {
                    "type": "term",
                    "value": "abc",
                    "leaf_type": "simple",
                    "grouped_by": []
                  },
                  {
                          "type": "integer",
                          "value": 4,
                          "leaf_type": "simple",
                          "grouped_by": []
                  },
                        {
                                "type": "integer",
                                "value": 3,
                                "leaf_type": "simple",
                                "grouped_by": []
                        }
                ],
                "funcname": {
                  "type": "term",
                  "value": "substr",
                  "leaf_type": "simple"
                },
                "leaf_type": "complex"
              },
              "arg_type": "tree",
              "group_by": [],
              "named_as": ""
            }
          ]
        }
      }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","abc":"abcdefgh","result":"def"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","abc":"abcdefgh","result":"def"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","abc":"abcdefgh","result":"def"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","abc":"abcdefgh","result":"def"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","abc":"abcdefgh","result":"def"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","abc":"abcdefgh","result":"def"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","abc":"abcdefgh","result":"def"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","abc":"abcdefgh","result":"def"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","abc":"abcdefgh","result":"def"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","abc":"abcdefgh","result":"def"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 13. Command: | eval tonumber") {
    val otlCommand =
      """{
      "name": "eval",
      "arguments": {
        "evaluations": [
          {
              "key": "numText",
              "type": "string",
              "value": "7483",
              "arg_type": "kwarg",
              "group_by": [],
              "named_as": ""
          }
        ]
      }
    },
    {
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
              "key": "result",
              "type": "function",
              "value": {
                "type": "function",
                "funcargs": [
                  {
                    "type": "term",
                    "value": "numText",
                    "leaf_type": "simple",
                    "grouped_by": []
                  }
                ],
                "funcname": {
                  "type": "term",
                  "value": "tonumber",
                  "leaf_type": "simple"
                },
                "leaf_type": "complex"
              },
              "arg_type": "tree",
              "group_by": [],
              "named_as": ""
            }
          ]
        }
      }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","numText":"7483","result":7483.0},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","numText":"7483","result":7483.0},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","numText":"7483","result":7483.0},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","numText":"7483","result":7483.0},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","numText":"7483","result":7483.0},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","numText":"7483","result":7483.0},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","numText":"7483","result":7483.0},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","numText":"7483","result":7483.0},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","numText":"7483","result":7483.0},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","numText":"7483","result":7483.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 14. Command: | eval tostring") {
    val otlCommand =
      """{
        |          "name": "eval",
        |          "arguments": {
        |            "evaluations": [
        |              {
        |                "key": "result",
        |                "type": "expr",
        |                "value": {
        |                    "op": "+",
        |                    "lhs": {
        |              "type": "function",
        |              "funcargs": [
        |                {
        |                  "type": "term",
        |                  "value": "_time",
        |                  "leaf_type": "simple",
        |                  "grouped_by": []
        |                }
        |              ],
        |              "funcname": {
        |                "type": "term",
        |                "value": "tostring",
        |                "leaf_type": "simple"
        |              },
        |              "leaf_type": "complex"
        |            },
        |                    "rhs": {
        |                      "type": "string",
        |                      "value": "asd",
        |                      "leaf_type": "simple"
        |                    },
        |                    "type": "expr",
        |                    "leaf_type": "complex"
        |                  },
        |                "arg_type": "tree",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |            ]
        |          }
        |        }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","result":"1568026476854asd"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","result":"1568026476855asd"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","result":"1568026476856asd"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","result":"1568026476857asd"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","result":"1568026476858asd"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","result":"1568026476859asd"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","result":"1568026476860asd"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","result":"1568026476861asd"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","result":"1568026476862asd"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","result":"1568026476863asd"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 15.0. Command: | eval true") {
    val otlCommand =
      """{
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
              "key": "result",
              "type": "function",
              "value": {
                "type": "function",
                "funcargs": [
                  {
                    "type": "term",
                    "value": "true()",
                    "leaf_type": "simple",
                    "grouped_by": []
                  },
                  {
                          "type": "integer",
                          "value": 1,
                          "leaf_type": "simple",
                          "grouped_by": []
                  },
                        {
                                "type": "integer",
                                "value": 0,
                                "leaf_type": "simple",
                                "grouped_by": []
                        }
                ],
                "funcname": {
                  "type": "term",
                  "value": "if",
                  "leaf_type": "simple"
                },
                "leaf_type": "complex"
              },
              "arg_type": "tree",
              "group_by": [],
              "named_as": ""
            }
          ]
        }
      }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","result":1},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","result":1},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","result":1},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","result":1},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","result":1},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","result":1},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","result":1},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","result":1},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","result":1},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","result":1}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 15.1. Command: | eval if equals") {
    val otlCommand =
    """{
      |        "name": "eval",
      |        "arguments": {
      |          "evaluations": [
      |            {
      |              "key": "result",
      |              "type": "function",
      |              "value": {
      |                "type": "function",
      |                "funcargs": [
      |             {
      |                "key": {
      |                  "type": "term",
      |                  "value": "_time",
      |                  "leaf_type": "simple"
      |                },
      |                "type": "kwarg",
      |                "value": {
      |                  "type": "integer",
      |                  "value": 1568026476859,
      |                  "leaf_type": "simple"
      |                },
      |                "leaf_type": "complex",
      |                "grouped_by": []
      |              },
      |                  {
      |                          "type": "integer",
      |                          "value": 1,
      |                          "leaf_type": "simple",
      |                          "grouped_by": []
      |                  },
      |                        {
      |                                "type": "integer",
      |                                "value": 2,
      |                                "leaf_type": "simple",
      |                                "grouped_by": []
      |                        }
      |                ],
      |                "funcname": {
      |                  "type": "term",
      |                  "value": "if",
      |                  "leaf_type": "simple"
      |                },
      |                "leaf_type": "complex"
      |              },
      |              "arg_type": "tree",
      |              "group_by": [],
      |              "named_as": ""
      |            }
      |          ]
      |        }
      |      }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","result":2},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","result":2},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","result":2},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","result":2},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","result":2},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","result":1},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","result":2},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","result":2},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","result":2},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","result":2}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  /*test("Test 16.0 Command: | eval case ") {
    val actual = execute(
      """stats values(random_Field) as val max(random_Field) as mR | eval res=case(1=1, mvzip(val,val,"--"), 1=1,1)"""
    )
    val expected =
      """[
        |{"val":["50","20","30","100","-90","-100","10","60","0"],"mR":"60","res":["50--50","20--20","30--30","100--100","-90---90","-100---100","10--10","60--60","0--0"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 16.1 Command: | eval case ") {
    val actual = execute(
      """|table junkField | eval res=case(1=1, split(junkField, "_"), 1=1,1)"""
    )
    val expected =
      """[
        |{"junkField":"q2W","res":["q2W"]},
        |{"junkField":"132_.","res":["132","."]},
        |{"junkField":"asd.cx","res":["asd.cx"]},
        |{"junkField":"XYZ","res":["XYZ"]},
        |{"junkField":"123_ASD","res":["123","ASD"]},
        |{"junkField":"casd(@#)asd","res":["casd(@#)asd"]},
        |{"junkField":"QQQ.2","res":["QQQ.2"]},
        |{"junkField":"00_3","res":["00","3"]},
        |{"junkField":"112","res":["112"]},
        |{"junkField":"word","res":["word"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 16.2 Command: | eval case ") {
    val actual = execute(
      """|table junkField | eval res=mvindex(case(1=1, split(junkField, "_"), 1=1,1),0)"""
    )
    val expected =
      """[
        |{"junkField":"q2W","res":["q2W"]},
        |{"junkField":"132_.","res":["132"]},
        |{"junkField":"asd.cx","res":["asd.cx"]},
        |{"junkField":"XYZ","res":["XYZ"]},
        |{"junkField":"123_ASD","res":["123"]},
        |{"junkField":"casd(@#)asd","res":["casd(@#)asd"]},
        |{"junkField":"QQQ.2","res":["QQQ.2"]},
        |{"junkField":"00_3","res":["00"]},
        |{"junkField":"112","res":["112"]},
        |{"junkField":"word","res":["word"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 16.3 Command: | eval case ") {
    val actual = execute(
      """|table junkField | eval res=mvindex(case(1=1,"qwer\\\"werty",1=1,split(junkField, "_")),0)"""
    )
    val expected =
      """[
        |{"junkField":"q2W","res":["qwer\\\"werty"]},
        |{"junkField":"132_.","res":["qwer\\\"werty"]},
        |{"junkField":"asd.cx","res":["qwer\\\"werty"]},
        |{"junkField":"XYZ","res":["qwer\\\"werty"]},
        |{"junkField":"123_ASD","res":["qwer\\\"werty"]},
        |{"junkField":"casd(@#)asd","res":["qwer\\\"werty"]},
        |{"junkField":"QQQ.2","res":["qwer\\\"werty"]},
        |{"junkField":"00_3","res":["qwer\\\"werty"]},
        |{"junkField":"112","res":["qwer\\\"werty"]},
        |{"junkField":"word","res":["qwer\\\"werty"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }*/

  test("Test 17.0 Command: | eval mvcount") { //TODO
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "sf",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 10,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "sf",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvcount",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":10},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":10},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":10},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":10},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":10},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":10},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":10},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":10},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":10},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","sf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":10}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 17.1 Command: | eval mvcount not mv") { //TODO
    val otlCommand =
      """{
        |      "name": "eval",
        |      "arguments": {
        |        "evaluations": [
        |          {
        |              "key": "sf",
        |              "type": "string",
        |              "value": "abc",
        |              "arg_type": "kwarg",
        |              "group_by": [],
        |              "named_as": ""
        |          }
        |        ]
        |      }
        |    },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "sf",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvcount",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","sf":"abc"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","sf":"abc"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","sf":"abc"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","sf":"abc"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","sf":"abc"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","sf":"abc"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","sf":"abc"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","sf":"abc"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","sf":"abc"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","sf":"abc"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  /*test("Test 18.0 Command: | eval f2=strptime(f1, format)") {
    val otlCommand =
      """{
        |      "name": "eval",
        |      "arguments": {
        |        "evaluations": [
        |          {
        |              "key": "f1",
        |              "type": "string",
        |              "value": "Mon Oct 21 05:00:00 EDT 2019",
        |              "arg_type": "kwarg",
        |              "group_by": [],
        |              "named_as": ""
        |          }
        |        ]
        |      }
        |    },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "f1",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "string",
        |                "value": "%a %b %d %H:%M:%S %Z %Y",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "strptime",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = execute(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","sf":"abc"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","sf":"abc"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","sf":"abc"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","sf":"abc"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","sf":"abc"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","sf":"abc"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","sf":"abc"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","sf":"abc"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","sf":"abc"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","sf":"abc"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")

    /*val actual = execute("""eval f1="Mon Oct 21 05:00:00 EDT 2019" | eval f2=strptime(f1, "%a %b %d %H:%M:%S %Z %Y")""")
    val expected =
      """[
        |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400},
        |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"f1":"Mon Oct 21 05:00:00 EDT 2019","f2":1571648400}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")*/
  }

  test("Test 18.1 Command: | eval f2=strptime(mv f1, format)") {
    val actual = execute("""eval f1="Mon Oct 21 05:00:00 EDT 2019" | stats values(f1) as f2 | eval f2 = mvindex(f2,0) | eval f3=strptime(f2, "%a %b %d %H:%M:%S %Z %Y")""")
    val expected =
      """[
        |{"f2":["Mon Oct 21 05:00:00 EDT 2019"],"f3":1571648400}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 18.2 Command: | eval f2=strptime(null f1, format)") {
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "f2",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 10,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "f2",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "string",
        |                "value": "%a %b %d %H:%M:%S %Z %Y",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "strptime",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = execute(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","sf":"abc"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","sf":"abc"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","sf":"abc"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","sf":"abc"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","sf":"abc"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","sf":"abc"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","sf":"abc"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","sf":"abc"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","sf":"abc"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","sf":"abc"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")

    /*val actual = execute("""stats values(f1) as f2  | eval f3=strptime(f2, "%a %b %d %H:%M:%S %Z %Y")""")
    val expected =
      """[
        |{"f2":[]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")*/
  }*/

  test("Test 18.3 Command: | eval x = _num_ + strptime(...)") {
    val otlCommand =
      """{
        |          "name": "eval",
        |          "arguments": {
        |            "evaluations": [
        |              {
        |                "key": "g",
        |                "type": "expr",
        |                "value": {
        |                    "op": "+",
        |                    "lhs": {
        |                      "type": "integer",
        |                      "value": 1,
        |                      "leaf_type": "simple"
        |            },
        |                    "rhs": {
        |                      "type": "function",
        |              "funcargs": [
        |                {
        |                  "type": "string",
        |                  "value": "2023-01-01",
        |                  "leaf_type": "simple",
        |                  "grouped_by": []
        |                },
        |                {
        |                  "type": "string",
        |                  "value": "%Y-%m-%d",
        |                  "leaf_type": "simple",
        |                  "grouped_by": []
        |                }
        |              ],
        |              "funcname": {
        |                "type": "term",
        |                "value": "strptime",
        |                "leaf_type": "simple"
        |              },
        |              "leaf_type": "complex"
        |                    },
        |                    "type": "expr",
        |                    "leaf_type": "complex"
        |                  },
        |                "arg_type": "tree",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |            ]
        |          }
        |        }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","g":1672520401},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","g":1672520401},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","g":1672520401},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","g":1672520401},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","g":1672520401},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","g":1672520401},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","g":1672520401},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","g":1672520401},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","g":1672520401},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","g":1672520401}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  /*test("Test 19. Command: | eval. 'Max' and 'min' in query should not replaced with 'array_max' and 'array_min'") {
    val actual = execute(""" eval max = 3 | eval min = 2 | eval newf = if(serialField > 5, max, min) | fields min, max, newf """)
    val expected =
      """[
        |{"min":2,"max":3,"newf":2},
        |{"min":2,"max":3,"newf":2},
        |{"min":2,"max":3,"newf":2},
        |{"min":2,"max":3,"newf":2},
        |{"min":2,"max":3,"newf":2},
        |{"min":2,"max":3,"newf":2},
        |{"min":2,"max":3,"newf":3},
        |{"min":2,"max":3,"newf":3},
        |{"min":2,"max":3,"newf":3},
        |{"min":2,"max":3,"newf":3}
    ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 20. Command: | eval. Expression with $ in column names should not fail ") {
    val actual = execute(
      """
        | eval $newcol$ = if(serialField > 4, 1, 0)
        | eval ifcol = if($newcol$ > 0, -10, -20)
        | eval evalcol = $newcol$ - 0.5
        | where $newcol$ > 0
        | fields $newcol$, ifcol, evalcol, serialField
    """)
    val expected =
      """[
        |{"$newcol$":1, "ifcol": -10, "evalcol":0.5, "serialField":"5"},
        |{"$newcol$":1, "ifcol": -10, "evalcol":0.5, "serialField":"6"},
        |{"$newcol$":1, "ifcol": -10, "evalcol":0.5, "serialField":"7"},
        |{"$newcol$":1, "ifcol": -10, "evalcol":0.5, "serialField":"8"},
        |{"$newcol$":1, "ifcol": -10, "evalcol":0.5, "serialField":"9"}
    ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }*/
  test("Test 21. Command: | eval \"Колонка с пробелами\" = \"123\"") {
    val otlCommand =
      """{
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
                "key": "Колонка с пробелами",
                "type": "string",
                "value": "123",
                "arg_type": "kwarg",
                "group_by": [],
                "named_as": ""
              }
          ]
        }
      }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","Колонка с пробелами":"123"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","Колонка с пробелами":"123"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","Колонка с пробелами":"123"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","Колонка с пробелами":"123"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","Колонка с пробелами":"123"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","Колонка с пробелами":"123"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","Колонка с пробелами":"123"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","Колонка с пробелами":"123"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","Колонка с пробелами":"123"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","Колонка с пробелами":"123"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 22. Command: | eval expr1, expr2") {
    val otlCommand =
      """{
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
                "key": "a",
                "type": "integer",
                "value": 1,
                "arg_type": "kwarg",
                "group_by": [],
                "named_as": ""
              },
            {
                "key": "b",
                      "type": "integer",
                      "value": 2,
                      "arg_type": "kwarg",
                      "group_by": [],
                      "named_as": ""
            }
          ]
        }
      }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_time":1568026476854,"a":1,"b":2},
        |{"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_time":1568026476855,"a":1,"b":2},
        |{"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_time":1568026476856,"a":1,"b":2},
        |{"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_time":1568026476857,"a":1,"b":2},
        |{"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_time":1568026476858,"a":1,"b":2},
        |{"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_time":1568026476859,"a":1,"b":2},
        |{"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_time":1568026476860,"a":1,"b":2},
        |{"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_time":1568026476861,"a":1,"b":2},
        |{"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_time":1568026476862,"a":1,"b":2},
        |{"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_time":1568026476863,"a":1,"b":2}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  /*test("Test 23. Command: | eval ambigous field") {
    val actual = execute(""" eval max = 3, min = 2, newf = if(serialField > 5, max, min), res = split(junkField, "_") | fields min, max, newf, res """)
    val expected =
      """[
        |{"min":2,"max":3,"newf":2,"res":["q2W"]},
        |{"min":2,"max":3,"newf":2,"res":["132","."]},
        |{"min":2,"max":3,"newf":2,"res":["asd.cx"]},
        |{"min":2,"max":3,"newf":2,"res":["XYZ"]},
        |{"min":2,"max":3,"newf":2,"res":["123","ASD"]},
        |{"min":2,"max":3,"newf":2,"res":["casd(@#)asd"]},
        |{"min":2,"max":3,"newf":3,"res":["QQQ.2"]},
        |{"min":2,"max":3,"newf":3,"res":["00","3"]},
        |{"min":2,"max":3,"newf":3,"res":["112"]},
        |{"min":2,"max":3,"newf":3,"res":["word"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 24. Command: | eval multiple expressions with multivalues") {
    val actual = execute("""  | eventstats avg(random_Field) as av1 | eval t = if(random_Field > av1 , 1, 0)  """)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","random_Field":"100","av1":13.0,"t":1},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","random_Field":"-90","av1":13.0,"t":0},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","random_Field":"50","av1":13.0,"t":1},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","random_Field":"20","av1":13.0,"t":1},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","random_Field":"30","av1":13.0,"t":1},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","random_Field":"50","av1":13.0,"t":1},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","random_Field":"60","av1":13.0,"t":1},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","random_Field":"-100","av1":13.0,"t":0},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","random_Field":"0","av1":13.0,"t":0},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","random_Field":"10","av1":13.0,"t":0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }*/

  test("Test 25. Command: | eval replace with $ in args") {
    val otlCommand =
      """{
        |      "name": "eval",
        |      "arguments": {
        |        "evaluations": [
        |          {
        |              "key": "Word",
        |              "type": "string",
        |              "value": "xnvtrs",
        |              "arg_type": "kwarg",
        |              "group_by": [],
        |              "named_as": ""
        |          }
        |        ]
        |      }
        |    },
        |{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "t1",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "Word",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "string",
        |                "value": ".+$",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "string",
        |                "value": "X",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "replace",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","Word":"xnvtrs","t1":"X"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","Word":"xnvtrs","t1":"X"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","Word":"xnvtrs","t1":"X"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","Word":"xnvtrs","t1":"X"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","Word":"xnvtrs","t1":"X"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","Word":"xnvtrs","t1":"X"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","Word":"xnvtrs","t1":"X"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","Word":"xnvtrs","t1":"X"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","Word":"xnvtrs","t1":"X"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","Word":"xnvtrs","t1":"X"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 26.0 Command: | eval mvfind") { //TODO
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "rf",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 10,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "rf",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "string",
        |                "value": "-.*",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvfind",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  /*test("Test 26.1 Command: | eval mvfind") { //TODO
    val otlCommand =
      """{
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "rf",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "integer",
        |                "value": 1,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "integer",
        |                "value": 10,
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvrange",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    "name": "eval",
        |    "arguments": {
        |      "evaluations": [
        |        {
        |          "key": "result",
        |          "type": "function",
        |          "value": {
        |            "type": "function",
        |            "funcargs": [
        |              {
        |                "type": "term",
        |                "value": "x",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              },
        |              {
        |                "type": "string",
        |                "value": "-.*",
        |                "leaf_type": "simple",
        |                "grouped_by": []
        |              }
        |            ],
        |            "funcname": {
        |              "type": "term",
        |              "value": "mvfind",
        |              "leaf_type": "simple"
        |            },
        |            "leaf_type": "complex"
        |          },
        |          "arg_type": "tree",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }""".stripMargin
    val actual = execute(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","rf":[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0],"result":-1}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")

    /*val actual = execute("""stats values(random_Field) as rf| eval result=mvfind(x, "-.*" )""")
    val expected =
      """[
        |{"rf":["50","20","30","100","-90","-100","10","60","0"]}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")*/
  }

  test("Test 26.2 Command: | eval mvfind") { //TODO
    val actual = execute("""stats values(random_Field) as rf| eval result=mvfind(rf, x )""")
    val expected =
      """[
        |{"rf":["50","20","30","100","-90","-100","10","60","0"],"result":-1}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }*/

  test("Test 27 Command: | eval <cyrillic_name>") {
    val otlCommand =
      """{
        "name": "eval",
        "arguments": {
          "evaluations": [
            {
                "key": "абв",
                "type": "integer",
                "value": 9,
                "arg_type": "kwarg",
                "group_by": [],
                "named_as": ""
              }
          ]
        }
      }"""
    val actual = executeQuery(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","абв":9},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","абв":9},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","абв":9},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","абв":9},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","абв":9},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","абв":9},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","абв":9},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","абв":9},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","абв":9},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","абв":9}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
