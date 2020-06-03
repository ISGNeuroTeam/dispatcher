package ot.scalaotl.commands

  class OTLTransposeTest extends CommandTest {

    override val dataset: String = """[
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"nullField\": \"\", \"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_nifi_time":"1568037188486", "nullField":"", "serialField":"0","random_Field":"100","WordField":"qwe","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"nullField\": \"\", \"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_nifi_time":"1568037188487", "nullField":"", "serialField":"1","random_Field":"-90","WordField":"rty","junkField":"132_.","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"nullField\": \"\", \"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_nifi_time":"1568037188487", "nullField":"", "serialField":"2","random_Field":"50","WordField":"uio","junkField":"asd.cx","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"nullField\": \"\", \"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_nifi_time":"1568037188487", "nullField":"", "serialField":"3","random_Field":"20","WordField":"GreenPeace","junkField":"XYZ","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"nullField\": \"\", \"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_nifi_time":"1568037188487", "nullField":"", "serialField":"4","random_Field":"30","WordField":"fgh","junkField":"123_ASD","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"}
  ]"""

    test("Test 1. Command: | transpose w/o options "   ) {
      val actual = execute("""| table serialField WordField junkField | transpose """)
      val expected = """[
                       |{"column":"WordField","row 1":"qwe","row 2":"rty","row 3":"uio","row 4":"GreenPeace","row 5":"fgh"},
                       |{"column":"junkField","row 1":"q2W","row 2":"132_.","row 3":"asd.cx","row 4":"XYZ","row 5":"123_ASD"},
                       |{"column":"serialField","row 1":"0","row 2":"1","row 3":"2","row 4":"3","row 5":"4"}
                       |]""".stripMargin
      assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
    }

    test("Test 2. Command: | transpose + 'column_name' option "   ) {
      val actual = execute("""| table serialField WordField junkField | transpose column_name="headers" """)
      val expected = """[
                       |{"\"headers\"":"WordField","row 1":"qwe","row 2":"rty","row 3":"uio","row 4":"GreenPeace","row 5":"fgh"},
                       |{"\"headers\"":"junkField","row 1":"q2W","row 2":"132_.","row 3":"asd.cx","row 4":"XYZ","row 5":"123_ASD"},
                       |{"\"headers\"":"serialField","row 1":"0","row 2":"1","row 3":"2","row 4":"3","row 5":"4"}
                       |]""".stripMargin
      assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
    }

    //При указании header_field  в кавычках сваливаемся с ошибкой.
    //При этом в другой опции этой же команды (см. например. test 1) значение опции в кавычках допускается.
    //При указании header_field без кавычек ошибки нет.
    ignore("Test 3. Command: | transpose + 'header_field' option "   ) {
      val actual = execute("""| table serialField WordField junkField | transpose header_field="junkField"  """)
      val expected = """[
                       |{"column":"WordField","123_ASD":"fgh","132_.":"rty","XYZ":"GreenPeace","asd.cx":"uio","q2W":"qwe"},
                       |{"column":"junkField","123_ASD":"123_ASD","132_.":"132_.","XYZ":"XYZ","asd.cx":"asd.cx","q2W":"q2W"},
                       |{"column":"serialField","123_ASD":"4","132_.":"1","XYZ":"3","asd.cx":"2","q2W":"0"}
                       |]""".stripMargin
      assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
    }

    //Нельзя (или не понятно как)  вывести количество результатов НЕ равному количеству по умолчанию (больше или меньше 5)
    //Если задать так, то количество результатов по прежнему 5, а нужно 3:  |  transpose head=3
    //Внимание! Сейчав в тесте Expected результат  "синтетический" - подготовленный "на глаз". Это значит, что после исправления бага  Expected результат теста может отличаться от Expected, который вписан в тест сейчас.
    ignore("Test 4. Command: | transpose  + non-default output number of results"   ) {
      val actual = execute("""| table serialField WordField junkField | transpose head=3  """)
      val expected = """[
                       |{"column":"WordField","row 1":"qwe","row 2":"rty","row 3":"uio"},
                       |{"column":"junkField","row 1":"q2W","row 2":"132_.","row 3":"asd.cx"},
                       |{"column":"serialField","row 1":"0","row 2":"1","row 3":"2"}
                       |]""".stripMargin
      assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
    }

    //При задании опции include_empty=false команда работает по прежнему в режиме include_empty=true
    //Внимание! Сейчав в тесте Expected результат  "синтетический" - подготовленный "на глаз". Это значит, что после исправления бага  Expected результат теста может отличаться от Expected, который вписан в тест сейчас.
    ignore("Test 5. Command: | transpose + 'include_empty' option "   ) {
      val actual = execute("""| table serialField nullField WordField  | transpose include_empty=false  """)
      val expected = """[
                       |{"column":"WordField","row 1":"qwe","row 2":"rty","row 3":"uio","row 4":"GreenPeace","row 5":"fgh"},
                       |{"column":"serialField","row 1":"0","row 2":"1","row 3":"2","row 4":"3","row 5":"4"}
                       |]""".stripMargin
      assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
    }

}
