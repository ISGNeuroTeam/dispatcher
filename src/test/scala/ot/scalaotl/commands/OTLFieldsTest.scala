package ot.scalaotl.commands

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => F}
import ot.scalaotl.Converter

import scala.collection.mutable.ListBuffer

class OTLFieldsTest extends CommandTest {

  def getIndexDF(index : String): DataFrame ={
    val filenames = ListBuffer[String]()
    try {
      val status = fs_disk.listStatus(new Path(s"$tmpDir/indexes/$index"))
      status.foreach(x => filenames += s"$tmpDir/indexes/$index/${x.getPath.getName}")
    }
    catch { case e: Exception => log.debug(e);}
    val df = spark.read.options(Map("recursiveFileLookup"->"true")).schema(datasetSchema)
      .parquet(filenames.seq: _*)
      .withColumn("index", F.lit(index))
    df
  }


  test("Test 1. Command: | head 1") {
    val query = createQuery("""fields _time, _meta, host, sourcetype, index""", 0, 0, "otstats")
    val actual = new Converter(query).run
    var expected = getIndexDF(test_index)
      .select(F.col("_time"), F.col("_meta"), F.col("host"), F.col("sourcetype"), F.col("index"))
    expected = setNullableStateOfColumn(expected, "index", true)
    compareDataFrames(actual, expected)
  }


  test("Test 0. Command: | head 1") {
    val actual = execute("""fields - _raw""")
    val expected =
      """[
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854},
                      {"_time":1568026476854}
]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
