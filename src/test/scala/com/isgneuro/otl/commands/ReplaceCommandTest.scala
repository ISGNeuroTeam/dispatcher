package com.isgneuro.otl.commands

class ReplaceCommandTest extends Command2Test {

  test("Test 0. Command: | replace <text1> as <text2> in <column>") {
    val otlCommand =
      """{
        |        "name": "eval",
        |        "arguments": {
        |          "evaluation": [
        |            {
        |                "key": "junkField",
        |                "type": "string",
        |                "value": "axdword",
        |                "arg_type": "kwarg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      },
        |{
        |    "name": "replace",
        |    "arguments": {
        |      "fieldValuePart": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "word",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "with": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "with",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "fieldReplacement": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "ZZZ",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "in": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "in",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "fieldName": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "junkField",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }
        |""".stripMargin
    val actual = execute(otlCommand)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","junkField":"axdZZZ"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","junkField":"axdZZZ"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","junkField":"axdZZZ"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","junkField":"axdZZZ"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","junkField":"axdZZZ"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","junkField":"axdZZZ"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","junkField":"axdZZZ"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","junkField":"axdZZZ"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","junkField":"axdZZZ"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","junkField":"axdZZZ"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | replace \" \" with \"\" in text") {
    val otl_command =
      """{
        |        "name": "eval",
        |        "arguments": {
        |          "evaluation": [
        |            {
        |                "key": "text",
        |                "type": "string",
        |                "value": "cat cat",
        |                "arg_type": "kwarg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      },
        |{
        |    "name": "replace",
        |    "arguments": {
        |      "fieldValuePart": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": " ",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "with": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "with",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "fieldReplacement": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "in": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "in",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "fieldName": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "text",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }
        |""".stripMargin
    val actual = execute(otl_command)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","text":"catcat"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","text":"catcat"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","text":"catcat"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","text":"catcat"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","text":"catcat"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","text":"catcat"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","text":"catcat"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","text":"catcat"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","text":"catcat"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","text":"catcat"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | replace \" \" with \" 2 \" in text") {
    val otl_command =
      """{
        |        "name": "eval",
        |        "arguments": {
        |          "evaluation": [
        |            {
        |                "key": "text",
        |                "type": "string",
        |                "value": "1 3",
        |                "arg_type": "kwarg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      },
        |{
        |    "name": "replace",
        |    "arguments": {
        |      "fieldValuePart": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": " ",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "with": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "with",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "fieldReplacement": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": " 2 ",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "in": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "in",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "fieldName": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "text",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }
        |""".stripMargin
    val actual = execute(otl_command)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","text":"1 2 3"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","text":"1 2 3"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","text":"1 2 3"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","text":"1 2 3"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","text":"1 2 3"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","text":"1 2 3"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","text":"1 2 3"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","text":"1 2 3"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","text":"1 2 3"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","text":"1 2 3"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | replace text with te=xt") {
    val otl_command =
      """{
        |        "name": "eval",
        |        "arguments": {
        |          "evaluation": [
        |            {
        |                "key": "eqField",
        |                "type": "string",
        |                "value": "defgh",
        |                "arg_type": "kwarg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      },
        |{
        |    "name": "replace",
        |    "arguments": {
        |      "fieldValuePart": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "defgh",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "with": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "with",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "fieldReplacement": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "d=efgh",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "in": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "in",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "fieldName": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "eqField",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }
        |""".stripMargin
    val actual = execute(otl_command)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","eqField":"d=efgh"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","eqField":"d=efgh"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","eqField":"d=efgh"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","eqField":"d=efgh"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","eqField":"d=efgh"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","eqField":"d=efgh"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","eqField":"d=efgh"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","eqField":"d=efgh"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","eqField":"d=efgh"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","eqField":"d=efgh"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  ignore("Test 4. Command: | replace word1 (word2 word3) with simple_word in text") {
    val otl_command =
      """{
        |        "name": "eval",
        |        "arguments": {
        |          "evaluation": [
        |            {
        |                "key": "bracesText",
        |                "type": "string",
        |                "value": "start finish a*bc",
        |                "arg_type": "kwarg",
        |                "group_by": [],
        |                "named_as": ""
        |              }
        |          ]
        |        }
        |      },
        |{
        |    "name": "replace",
        |    "arguments": {
        |      "fieldValuePart": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "start finish '('a*bc",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "with": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "with",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "fieldReplacement": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "c_action",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "in": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "in",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ],
        |      "fieldName": [
        |        {
        |          "key": "",
        |          "type": "term",
        |          "value": "bracesText",
        |          "arg_type": "arg",
        |          "group_by": [],
        |          "named_as": ""
        |        }
        |      ]
        |    }
        |  }
        |""".stripMargin
    val actual = execute(otl_command)
    val expected =
      """[
        |{"_time":1568026476854,"_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","eqField":"d=efgh"},
        |{"_time":1568026476855,"_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","eqField":"d=efgh"},
        |{"_time":1568026476856,"_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","eqField":"d=efgh"},
        |{"_time":1568026476857,"_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","eqField":"d=efgh"},
        |{"_time":1568026476858,"_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","eqField":"d=efgh"},
        |{"_time":1568026476859,"_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","eqField":"d=efgh"},
        |{"_time":1568026476860,"_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","eqField":"d=efgh"},
        |{"_time":1568026476861,"_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","eqField":"d=efgh"},
        |{"_time":1568026476862,"_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","eqField":"d=efgh"},
        |{"_time":1568026476863,"_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","eqField":"d=efgh"}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
