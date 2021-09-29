package ot.dispatcher

import ot.scalaotl.Converter
import ot.scalaotl.commands.CommandTest

class CacheManagerTest extends CommandTest {

  test("Test 0. Make and load cache.") {

    val ssQuery = createQuery("eval SF=serialField*10 | eval nullField = null | table serialField, SF, NCol, nullField")
    val queryDF = new Converter(ssQuery).run

    val cacheManager = new CacheManager(spark)
    cacheManager.makeCache(queryDF, 1)

    val subDF = cacheManager.loadCache(1)

    queryDF.show()
    subDF.show()

    val actual = queryDF.toJSON.collect().mkString("[\n", ",\n", "\n]")
    val expected = subDF.toJSON.collect().mkString("[\n", ",\n", "\n]")

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")

  }

  test("Test 1. Test udf exception") {

    val ssQuery = createQuery("""eval f1="Mon Oct 21 05:00:00 EDT 2019" | eval f2=strptime(f1, "%a-%b %d %H:%M:%S %Z %Y")""")
    val queryDF = new Converter(ssQuery).run

    val cacheManager = new CacheManager(spark)
    val thrown = intercept[Exception] {
      cacheManager.makeCache(queryDF, 2)
    }
    assert(thrown.getMessage.endsWith("Unparseable date: \"Mon Oct 21 05:00:00 EDT 2019\""))
  }
}
