package ot.scalaotl.commands

class SearchTimeFieldExtractionTest extends CommandTest {

  override val dataset: String = """[ 
                                   |    {"_time":1570007900,"_raw":"{\"_time\":1570007900, \"tex.tFie.ld\":\"abc\",\"num\":[10,100], \"text\":[\"RUB.0\" ,\"RUB.00\"], \"tex.t3\": [{\"t1\": [{\"ab.c\":\"cba\"}, {\"ab.c\":\"cba\"}] }, {\"t2\": \"x1\"}]} "},
                                   |    {"_time":1570008000,"_raw":"{\"_time\":1570007900, \"textField\":\"abc\",\"num\":[10,100], \"text\":[\"RUB.0\" ,\"RUB.00\"], \"text3\": [{\"t1\": [{\"abc\":\"cba\"}] }, {\"t2\": \"x1\"}]}"},
                                   |    {"_time":1570008100,"_raw":"{\"_time\":1570008100, \"num\":[30,300], \"text\":[\"EUR.0\" ,\"EUR.00\"] }"},
                                   |    {"_time":1570008200,"_raw":"{\"_time\":1570008200, \"num\":[40,400], \"text\":[\"GPB.0\",\"GPB.00\"] }"},
                                   |    {"_time":1570008300,"_raw":"{\"_time\":1570008300, \"num\":[50,500], \"text\":[\"DRM.0\" ,\"DRM.00\"] }"}
                                   |  ]"""

  test("Search time field extraction| TEST 0. Search extracted nullable value fields "){
    val query = new OTLQuery(
      id = 0,
      original_otl = f"search index=* | table tex.tFie.ld, text{}",
      service_otl = "| read {\"" + test_index + "\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}} | table tex.tFie.ld, text{} ",
      tws = 0,
      twf = 0,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = true,
      preview = false
    )

    val actual = execute(query)
    val expected = """[
                     |{"tex.tFie.ld":"abc","text{}":["RUB.0","RUB.00"]},
                     |{"text{}":["RUB.0","RUB.00"]},
                     |{"text{}":["EUR.0","EUR.00"]},
                     |{"text{}":["GPB.0","GPB.00"]},
                     |{"text{}":["DRM.0","DRM.00"]}
                     |]""".stripMargin

    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
  }
  test("Search time field extraction| TEST 1.Search extracted multivalue fields "){
    val query = new OTLQuery(
      id = 0,
      original_otl = f"search index=* |table num{}, text{}",
      service_otl = "| read {\"" + test_index + "\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}} | table num{}, text{}",
      tws = 0,
      twf = 0,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = true,
      preview = false
    )

    val actual = execute(query)
    val expected = """[
                     |{"num{}":["10","100"],"text{}":["RUB.0","RUB.00"]},
                     |{"num{}":["10","100"],"text{}":["RUB.0","RUB.00"]},
                     |{"num{}":["30","300"],"text{}":["EUR.0","EUR.00"]},
                     |{"num{}":["40","400"],"text{}":["GPB.0","GPB.00"]},
                     |{"num{}":["50","500"],"text{}":["DRM.0","DRM.00"]}
                     |]""".stripMargin

    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
  }

  test("Search time field extraction| TEST 2. Search extracted multivalue fields "){
    val query = new OTLQuery(
      id = 0,
      original_otl = f"search index=* | table tex.t3{}.t1{}.ab.c, tex.t3.t1.ab.c, text3{}.t1{}.abc, text3.t1.abc, textField ",
      service_otl = "| read {\"" + test_index + "\": {\"query\": \"\", \"tws\": 0, \"twf\": 0}} | table tex.t3{}.t1{}.ab.c, tex.t3.t1.ab.c, text3{}.t1{}.abc, text3.t1.abc, textField ",
      tws = 0,
      twf = 0,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = true,
      preview = false
    )

    val actual = execute(query)
    val expected = """[
                     |{"tex.t3{}.t1{}.ab.c":["cba","cba"]},
                     |{"text3{}.t1{}.abc":["cba"],"textField":"abc"},
                     |{},
                     |{},
                     |{}
                     |]""".stripMargin

    assert(jsonCompare(actual, expected), f"\nResult : $actual\n---\nExpected : $expected")
  }

}
