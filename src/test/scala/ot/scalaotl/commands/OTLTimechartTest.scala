package ot.scalaotl.commands

class OTLTimechartTest extends CommandTest {

  override val dataset: String = """[ 
    {"_time":1570008000,"text":"RUB","num":1,"num[1].val":10,"num[2].val":100,"text[1].val":"RUB.0","text[2].val":"RUB.00","_raw":"_time=1570008000 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 "},
    {"_time":1570008020,"text":"USD","num":2,"num[1].val":20,"num[2].val":200,"text[1].val":"USD.0","text[2].val":"USD.00","_raw":"_time=1570008020 text=USD num=2 num{1}.val=20 num{2}.val=200 text{1}.val=USD.0 text{2}.val=USD.00 "},
    {"_time":1570008040,"text":"EUR","num":3,"num[1].val":30,"num[2].val":300,"text[1].val":"EUR.0","text[2].val":"EUR.00","_raw":"_time=1570008040 text=EUR num=3 num{1}.val=30 num{2}.val=300 text{1}.val=EUR.0 text{2}.val=EUR.00 "},
    {"_time":1570008060,"text":"GPB","num":4,"num[1].val":40,"num[2].val":400,"text[1].val":"GPB.0","text[2].val":"GPB.00","_raw":"_time=1570008060 text=GPB num=4 num{1}.val=40 num{2}.val=400 text{1}.val=GPB.0 text{2}.val=GPB.00 "},
    {"_time":1570008080,"text":"DRM","num":5,"num[1].val":50,"num[2].val":500,"text[1].val":"DRM.0","text[2].val":"DRM.00","_raw":"_time=1570008080 text=DRM num=5 num{1}.val=50 num{2}.val=500 text{1}.val=DRM.0 text{2}.val=DRM.00 "},
    {"_time":1570008100,"text":"RUB","num":1,"num[1].val":10,"num[2].val":100,"text[1].val":"RUB.0","text[2].val":"RUB.00","_raw":"_time=1570008100 text=RUB num=1 num{1}.val=10 num{2}.val=100 text{1}.val=RUB.0 text{2}.val=RUB.00 "},
    {"_time":1570008120,"text":"USD","num":2,"num[1].val":20,"num[2].val":200,"text[1].val":"USD.0","text[2].val":"USD.00","_raw":"_time=1570008120 text=USD num=2 num{1}.val=20 num{2}.val=200 text{1}.val=USD.0 text{2}.val=USD.00 "},
    {"_time":1570008140,"text":"EUR","num":3,"num[1].val":30,"num[2].val":300,"text[1].val":"EUR.0","text[2].val":"EUR.00","_raw":"_time=1570008140 text=EUR num=3 num{1}.val=30 num{2}.val=300 text{1}.val=EUR.0 text{2}.val=EUR.00 "},
    {"_time":1570008160,"text":"GPB","num":4,"num[1].val":40,"num[2].val":400,"text[1].val":"GPB.0","text[2].val":"GPB.00","_raw":"_time=1570008160 text=GPB num=4 num{1}.val=40 num{2}.val=400 text{1}.val=GPB.0 text{2}.val=GPB.00 "},
    {"_time":1570008180,"text":"DRM","num":5,"num[1].val":50,"num[2].val":500,"text[1].val":"DRM.0","text[2].val":"DRM.00","_raw":"_time=1570008180 text=DRM num=5 num{1}.val=50 num{2}.val=500 text{1}.val=DRM.0 text{2}.val=DRM.00 "}
  ]"""

  test("Test 1. Command: | timechart. Multiple words in 'as' statement ") {
    val actual = execute(""" timechart span=1min count as "two words" """, jsonToDf(dataset))
    val expected = """[
                     |{"_time":1570008000, "two words":3},
                     |{"_time":1570008060, "two words":3},
                     |{"_time":1570008120, "two words":3},
                     |{"_time":1570008180, "two words":1}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }  

  test("Test 2. Command: | timechart. Fields used ") {

    val actual = getFieldsUsed(
      """ timechart span=1min count as "two words", max(num), first(eval(if(num > 3, 'text{1}.val', 'text{2}.val'))) as ff """
    )
    // Must be sorted lexicographically, separated with ', ' (comma + space)
    // Ex.: """ _raw, _time """
    val expected = """ __fake__, num, text{1}.val, text{2}.val """.trim
    assert(actual == expected, f"\nResult : $actual\n---\nExpected : $expected")    
  }

  test("Test 3. Command: | timechart. Multiple functions + 'by' ") {

    val actual = execute("""eval num=tonumber(num) | timechart span=1min count as cnt, max(num) as maxnum by text """, jsonToDf(dataset))
    val expected = """[
                     |{"_time":1570008000,"cnt: EUR":1,"maxnum: EUR":3.0,"cnt: RUB":1,"maxnum: RUB":1.0,"cnt: USD":1,"maxnum: USD":2.0},
                     |{"_time":1570008060,"cnt: DRM":1,"maxnum: DRM":5.0,"cnt: GPB":1,"maxnum: GPB":4.0,"cnt: RUB":1,"maxnum: RUB":1.0},
                     |{"_time":1570008120,"cnt: EUR":1,"maxnum: EUR":3.0,"cnt: GPB":1,"maxnum: GPB":4.0,"cnt: USD":1,"maxnum: USD":2.0},
                     |{"_time":1570008180,"cnt: DRM":1,"maxnum: DRM":5.0}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
