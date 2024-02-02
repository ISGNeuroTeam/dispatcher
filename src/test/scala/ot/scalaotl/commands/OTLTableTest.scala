package ot.scalaotl.commands

class OTLTableTest extends CommandTest {
   override val dataset: String = """[
  {"_time":1570007900,"_raw":"{\"_time\":1570007900, \"col1\": \"value\", \"Колонка\": \"1\", \"Колонка2\" : \"2\"}","col1": "value", "Колонка": "1", "Колонка2" : "2"}
]"""


  test("Test 0. Command: | table _time, _raw | Simple table command ") {
    val actual = execute("""table _time, _raw""")
    val expected = """[
                     |{"_time":1570007900,"_raw":"{\"_time\":1570007900, \"col1\": \"value\", \"Колонка\": \"1\", \"Колонка2\" : \"2\"}"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | table _time, _raw, Колонка | Simple table command with cyrillic name in column") {
    val actual = execute("""table _time, _raw, Колонка""")
    val expected = """[
                     |{"_time":1570007900,"_raw":"{\"_time\":1570007900, \"col1\": \"value\", \"Колонка\": \"1\", \"Колонка2\" : \"2\"}","Колонка":"1"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("""Test 2. Command: | rename Колонка as "Колонка с пробелами" | table _time, _raw, "Колонка с пробелами" | Simple table command with cyrillic name and spaces in column""") {
    
    val actual = execute(""" rename Колонка as "Колонка с пробелами" |table _time, _raw, "Колонка с пробелами" """)
    val expected = """[
                     |{"_time":1570007900,"_raw":"{\"_time\":1570007900, \"col1\": \"value\", \"Колонка\": \"1\", \"Колонка2\" : \"2\"}","Колонка с пробелами":"1"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
  
  test("""Test 3. Command: | rename Колонка as "Колонка с пробелами", Колонка2 as "Колонка с пробелами 2" | table
 "Колонка с пробелами", "Колонка с пробелами 2"  """) {
    
    val actual = execute("""  rename Колонка as "Колонка с пробелами", Колонка2 as "Колонка с пробелами 2" | table "Колонка с пробелами", "Колонка с пробелами 2"""")
    val expected = """[
    { "Колонка с пробелами": "1","Колонка с пробелами 2": "2" }
    ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
  test("Test 4. Command: | table non-existing | ") {
    val actual = execute("""table non-existing""")
    val expected = """[
    {}
    ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Command | table *") {
    
    val actual = execute("""table *""")
    val expected = """[
                     |{"_raw":"{\"_time\":1570007900, \"col1\": \"value\", \"Колонка\": \"1\", \"Колонка2\" : \"2\"}","_time":1570007900,"col1":"value","Колонка2":"2","Колонка":"1"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
  test("Test 6. Command | table Кол*") {
    
     val actual = execute("""table  Кол*""")
     val expected = """[
     {"Колонка2":"2","Колонка":"1"}
     ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
   }
  test("Test 7. Command | table *олон*") {
    
     val actual = execute("""table  *олон*""")
     val expected = """[
     {"Колонка2":"2","Колонка":"1"}
     ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
   }

  test("Test 8. Command | table *олон*") {
     val actual = execute("""eval "Русское поле экспериментов" = "123" | table "Русское поле экспериментов" """)
     val expected = """[
      {"Русское поле экспериментов":"123"}
     ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
   }
}
