package ot.scalaotl.commands

class RenameCommandTest extends Command2Test {

  ignore("Test 0"){
    val actual = execute("""[{
                           |    "name": "makeresults",
                           |    "arguments": {}
                           |  },
                           |  {
                           |    "name": "rename",
                           |    "arguments": {
                           |      "field": [
                           |        {
                           |          "key": "",
                           |          "type": "term",
                           |          "value": "_time",
                           |          "arg_type": "arg",
                           |          "group_by": [],
                           |          "named_as": "t"
                           |        }
                           |      ]
                           |    }
                           |  }]""".stripMargin)

  }
}
