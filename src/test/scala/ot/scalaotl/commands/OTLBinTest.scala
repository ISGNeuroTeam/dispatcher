package ot.scalaotl.commands

class OTLBinTest extends CommandTest {

  override val dataset: String = """[
    {"_time":1570008000,"text":"RUB","num":1,"num[1].val":10,"num[2].val":100,"text[1].val":"RUB.0","text[2].val":"RUB.00","_raw":"_time=1570008000 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 "},
    {"_time":1570008000,"text":"USD","num":2,"num[1].val":20,"num[2].val":200,"text[1].val":"USD.0","text[2].val":"USD.00","_raw":"_time=1570008020 text=USD num=2 num{1}.val=20 num{2}.val=200 text{1}.val=USD.0 text{2}.val=USD.00 "},
    {"_time":1570008000,"text":"EUR","num":3,"num[1].val":30,"num[2].val":300,"text[1].val":"EUR.0","text[2].val":"EUR.00","_raw":"_time=1570008040 text=EUR num=3 num{1}.val=30 num{2}.val=300 text{1}.val=EUR.0 text{2}.val=EUR.00 "},
    {"_time":1570008060,"text":"GPB","num":4,"num[1].val":40,"num[2].val":400,"text[1].val":"GPB.0","text[2].val":"GPB.00","_raw":"_time=1570008060 text=GPB num=4 num{1}.val=40 num{2}.val=400 text{1}.val=GPB.0 text{2}.val=GPB.00 "},
    {"_time":1570008060,"text":"DRM","num":5,"num[1].val":50,"num[2].val":500,"text[1].val":"DRM.0","text[2].val":"DRM.00","_raw":"_time=1570008080 text=DRM num=5 num{1}.val=50 num{2}.val=500 text{1}.val=DRM.0 text{2}.val=DRM.00 "},
    {"_time":1570008060,"text":"RUB","num":1,"num[1].val":10,"num[2].val":100,"text[1].val":"RUB.0","text[2].val":"RUB.00","_raw":"_time=1570008100 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 "},
    {"_time":1570008120,"text":"USD","num":2,"num[1].val":20,"num[2].val":200,"text[1].val":"USD.0","text[2].val":"USD.00","_raw":"_time=1570008120 text=USD num=2 num{1}.val=20 num{2}.val=200 text{1}.val=USD.0 text{2}.val=USD.00 "},
    {"_time":1570008120,"text":"EUR","num":3,"num[1].val":30,"num[2].val":300,"text[1].val":"EUR.0","text[2].val":"EUR.00","_raw":"_time=1570008140 text=EUR num=3 num{1}.val=30 num{2}.val=300 text{1}.val=EUR.0 text{2}.val=EUR.00 "},
    {"_time":1570008120,"text":"GPB","num":4,"num[1].val":40,"num[2].val":400,"text[1].val":"GPB.0","text[2].val":"GPB.00","_raw":"_time=1570008160 text=GPB num=4 num{1}.val=40 num{2}.val=400 text{1}.val=GPB.0 text{2}.val=GPB.00 "},
    {"_time":1570008120,"text":"DRM","num":5,"num[1].val":50,"num[2].val":500,"text[1].val":"DRM.0","text[2].val":"DRM.00","_raw":"_time=1570008180 text=DRM num=5 num{1}.val=50 num{2}.val=500 text{1}.val=DRM.0 text{2}.val=DRM.00 "}
  ]"""

  test("Test 1. Command: | 'Span' and 'as' options test") {
    val actual = execute(""" | table _time text num  | bin span=1m _time  as TIME""")
    val expected = """[
                     |{"_time":1570008000,"text":"RUB","num":"1","TIME":1570008000},
                     |{"_time":1570008000,"text":"USD","num":"2","TIME":1570008000},
                     |{"_time":1570008000,"text":"EUR","num":"3","TIME":1570008000},
                     |{"_time":1570008060,"text":"GPB","num":"4","TIME":1570008060},
                     |{"_time":1570008060,"text":"DRM","num":"5","TIME":1570008060},
                     |{"_time":1570008060,"text":"RUB","num":"1","TIME":1570008060},
                     |{"_time":1570008120,"text":"USD","num":"2","TIME":1570008120},
                     |{"_time":1570008120,"text":"EUR","num":"3","TIME":1570008120},
                     |{"_time":1570008120,"text":"GPB","num":"4","TIME":1570008120},
                     |{"_time":1570008120,"text":"DRM","num":"5","TIME":1570008120}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | 'bins' and 'as' options test") {
    val actual = execute("""  table _time text num | bin bins=3 num as NUM""")
    val expected = """[
                     |{"_time":1570008000,"text":"RUB","NUM":1.0},
                     |{"_time":1570008000,"text":"USD","NUM":1.0},
                     |{"_time":1570008000,"text":"EUR","NUM":2.333333333333333},
                     |{"_time":1570008060,"text":"GPB","NUM":3.6666666666666665},
                     |{"_time":1570008060,"text":"DRM","NUM":5.0},
                     |{"_time":1570008060,"text":"RUB","NUM":1.0},
                     |{"_time":1570008120,"text":"USD","NUM":1.0},
                     |{"_time":1570008120,"text":"EUR","NUM":2.333333333333333},
                     |{"_time":1570008120,"text":"GPB","NUM":3.6666666666666665},
                     |{"_time":1570008120,"text":"DRM","NUM":5.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
