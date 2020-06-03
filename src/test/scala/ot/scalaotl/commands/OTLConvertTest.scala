package ot.scalaotl.commands

class OTLConvertTest extends CommandTest {

  override val dataset: String = """[
    {"_time":1570008000,"text":"RUB","num":"1a","num1":"1a0","num[2].val":"10a0","text[1].val":"RUB.0","text[2].val":"RUB.00","_raw":"_time=1570008000 text=RUB num=1a num1=1a0 num{2}.val=10a0 text{1}.val=RUB.0 text{2}.val=RUB.00 "}
  ]"""

  //Некорректный разбор unixtime-времени по заданному шаблону
  //Описание: Если задать timeformat="%Y-%m-%d %H-%M-%S", то он разбирается в "2019-10-02", а должно в "2019-10-02 12:20:00".
  //Т.е. пропадает время.
  //При этом, если убрать пробел между датой и временем ("%Y-%m-%d%H-%M-%S"), то время появлятся - "2019-10-0212:20:00". Но, естественно, без пробела.
  ignore("Test 1. Command: | ctime with timeformat options") {
    val actual = execute(""" | table _time T | convert timeformat="%Y-%m-%d %H-%M-%S"  ctime(_time) as T""")
    val expected = """[
                     | {"_time":1570008000,"T":"\"2019-10-02 12:20:00"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
  //Некооректная конвертация поля времени (ctime(_time)) без явного указания timeformat
  ignore("Test 2. Command: | ctime with empty timeformat option") {
    val actual = execute(""" convert  ctime(_time) as T""")
    val expected = """[
                     |{"_time":1570008000,"_raw":"_time=1570008000 text=RUB num=1a num1=1a0 num{2}.val=10a0 text{1}.val=RUB.0 text{2}.val=RUB.00 ","T":"2019-10-02 12:20:00"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  //При попытке сконвертировать несколько полей с использованием параметра num() получаем пустой результат  - похоже, становятся NULL.
  //Такой же  результат получаю на платформе через SPlunk и EVA.
  //Однако, если убрать в примере "table num num1" перед convert, то результатом будут неизмененные исходные данные.
  ignore("Test 3. Command: | convert + num() option with multiple fields") {
    val actual = execute(""" table num num1 |  convert num(num) num(num1) """)
    val expected = """""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  //При попытке конвертировать такое "составное" поле сваливаемся с ошибкой:
  //20/04/14 12:22:12 ERROR ot.scalaspl.commands.SplBaseCommand.safeTransform(basecmd.scala:138) SplConvert: Error in  'convert' command: cannot resolve '`num{2}.val`' given input columns: [num, _time, num{2}.val, num1, _raw];;
  //Caused by: org.apache.spark.sql.AnalysisException: cannot resolve '`num{2}.val`' given input columns: [_time, _raw, num{2}.val];;
  //Внимание-! Дополнить документацию команды опцией num() после устранения бага.
  ignore("Test 4. Command: | convert + num() option with 'complicated' field") {
    val actual = execute(""" convert num(num{2}.val) """)
    val expected = """[
                     |{"_time":1570008000,"_raw":"_time=1570008000 text=RUB num=1 num1=1 num{2}.val=10a0 text{1}.val=RUB.0 text{2}.val=RUB.00 "}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}

