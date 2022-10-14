package ot.scalaotl.commands

class OTLDedupTest extends CommandTest {

  override val dataset = """[
    {"_time":"1570008000","dest_country":"Italy","sum":10,"text":"EUR","_raw":"_time=1570008000 dest_country=Italy text=EUR sum=10"},
    {"_time":"1570008001","dest_country":"Russia","sum":13,"text":"USD","_raw":"_time=1570008001 dest_country=Russia text=USD sum=13"},
    {"_time":"1570008002","dest_country":"Canada","sum":14,"text":"RUB","_raw":"_time=1570008002 dest_country=Canada text=RUB sum=14"},
    {"_time":"1570008003","dest_country":"Canada","sum":1,"text":"EUR","_raw":"_time=1570008003 dest_country=Canada text=EUR sum=1"},
    {"_time":"1570008004","dest_country":"Russia","sum":3,"text":"USD","_raw":"_time=1570008004 dest_country=Russia text=USD sum=3"},
    {"_time":"1570008005","dest_country":"Italy","sum":4,"text":"RUB","_raw":"_time=1570008005 dest_country=Italy text=RUB sum=4"},
    {"_time":"1570008006","dest_country":"Italy","sum":5,"text":"RUB","_raw":"_time=1570008006 dest_country=Italy text=RUB sum=5"},
    {"_time":"1570008007","dest_country":"Russia","sum":22,"text":"USD","_raw":"_time=1570008007 dest_country=Russia text=USD sum=22"},
    {"_time":"1570008008","dest_country":"Canada","sum":5,"text":"EUR","_raw":"_time=1570008008 dest_country=Canada text=EUR sum=5"}
  ]"""

  test("Test 1. Command: | dedup by single field ") {
    val actual = execute(""" dedup dest_country""")
    val expected = """[
                     |{"_time":1570008001,"_raw":"_time=1570008001 dest_country=Russia text=USD sum=13","dest_country":"Russia"},
                     |{"_time":1570008000,"_raw":"_time=1570008000 dest_country=Italy text=EUR sum=10","dest_country":"Italy"},
                     |{"_time":1570008002,"_raw":"_time=1570008002 dest_country=Canada text=RUB sum=14","dest_country":"Canada"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | dedup by multiple fields ") {
    val actual = execute(""" dedup dest_country text""")
    val expected = """[
                     |{"_time":1570008005,"_raw":"_time=1570008005 dest_country=Italy text=RUB sum=4","dest_country":"Italy","text":"RUB"},
                     |{"_time":1570008003,"_raw":"_time=1570008003 dest_country=Canada text=EUR sum=1","dest_country":"Canada","text":"EUR"},
                     |{"_time":1570008000,"_raw":"_time=1570008000 dest_country=Italy text=EUR sum=10","dest_country":"Italy","text":"EUR"},
                     |{"_time":1570008001,"_raw":"_time=1570008001 dest_country=Russia text=USD sum=13","dest_country":"Russia","text":"USD"},
                     |{"_time":1570008002,"_raw":"_time=1570008002 dest_country=Canada text=RUB sum=14","dest_country":"Canada","text":"RUB"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | dedup by single field with 'sort by' desc option ") {
    val actual = execute("""dedup dest_country sortby -sum | table dest_country, text, sum""")
    val expected = """[
                     |{"dest_country":"Canada","text":"RUB","sum":"14"},
                     |{"dest_country":"Russia","text":"USD","sum":"13"},
                     |{"dest_country":"Italy","text":"EUR","sum":"10"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4. Command: | dedup with 'consecutive' option ") {
    val actual = execute(""" | dedup dest_country consecutive=true """)
    val expected = """[
                     |{"_time":1570008000,"_raw":"_time=1570008000 dest_country=Italy text=EUR sum=10","dest_country":"Italy"},
                     |{"_time":1570008001,"_raw":"_time=1570008001 dest_country=Russia text=USD sum=13","dest_country":"Russia"},
                     |{"_time":1570008002,"_raw":"_time=1570008002 dest_country=Canada text=RUB sum=14","dest_country":"Canada"},
                     |{"_time":1570008004,"_raw":"_time=1570008004 dest_country=Russia text=USD sum=3","dest_country":"Russia"},
                     |{"_time":1570008005,"_raw":"_time=1570008005 dest_country=Italy text=RUB sum=4","dest_country":"Italy"},
                     |{"_time":1570008007,"_raw":"_time=1570008007 dest_country=Russia text=USD sum=22","dest_country":"Russia"},
                     |{"_time":1570008008,"_raw":"_time=1570008008 dest_country=Canada text=EUR sum=5","dest_country":"Canada"}
                     |]
                     """.stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Command: | dedup by multiple fields with 'sortby' option") {
    val actual = execute(""" dedup dest_country text sortby sum | table dest_country, text, sum""")
    val expected = """[
                     |{"dest_country":"Canada","text":"EUR","sum":"1"},
                     |{"dest_country":"Italy","text":"EUR","sum":"10"},
                     |{"dest_country":"Russia","text":"USD","sum":"13"},
                     |{"dest_country":"Canada","text":"RUB","sum":"14"},
                     |{"dest_country":"Italy","text":"RUB","sum":"4"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 6. Command: | dedup by multiple fields with 'consecutive' option") {
    val actual = execute(""" dedup dest_country text consecutive=true""")
    val expected = """[
                     |{"_time":1570008000,"_raw":"_time=1570008000 dest_country=Italy text=EUR sum=10","dest_country":"Italy","text":"EUR"},
                     |{"_time":1570008001,"_raw":"_time=1570008001 dest_country=Russia text=USD sum=13","dest_country":"Russia","text":"USD"},
                     |{"_time":1570008002,"_raw":"_time=1570008002 dest_country=Canada text=RUB sum=14","dest_country":"Canada","text":"RUB"},
                     |{"_time":1570008003,"_raw":"_time=1570008003 dest_country=Canada text=EUR sum=1","dest_country":"Canada","text":"EUR"},
                     |{"_time":1570008004,"_raw":"_time=1570008004 dest_country=Russia text=USD sum=3","dest_country":"Russia","text":"USD"},
                     |{"_time":1570008005,"_raw":"_time=1570008005 dest_country=Italy text=RUB sum=4","dest_country":"Italy","text":"RUB"},
                     |{"_time":1570008007,"_raw":"_time=1570008007 dest_country=Russia text=USD sum=22","dest_country":"Russia","text":"USD"},
                     |{"_time":1570008008,"_raw":"_time=1570008008 dest_country=Canada text=EUR sum=5","dest_country":"Canada","text":"EUR"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 7. Command: | dedup with 'consecutive' and 'sort by' options ") {
    val actual = execute(""" | dedup dest_country consecutive=true sortby sum | table dest_country, text, sum """)
    val expected = """[
                     |{"dest_country":"Italy","text":"EUR","sum":"10"},
                     |{"dest_country":"Russia","text":"USD","sum":"13"},
                     |{"dest_country":"Canada","text":"RUB","sum":"14"},
                     |{"dest_country":"Russia","text":"USD","sum":"22"},
                     |{"dest_country":"Russia","text":"USD","sum":"3"},
                     |{"dest_country":"Italy","text":"RUB","sum":"4"},
                     |{"dest_country":"Canada","text":"EUR","sum":"5"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
