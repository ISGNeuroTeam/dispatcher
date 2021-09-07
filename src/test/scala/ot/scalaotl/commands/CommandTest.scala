package ot.scalaotl.commands

import java.io.{File, PrintWriter}
import java.util.UUID

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import ot.AppConfig.{config, getLogLevel}
import ot.dispatcher.OTLQuery
import ot.scalaotl.Converter
import ot.scalaotl.extensions.StringExt._

import scala.reflect.io.Directory

abstract class CommandTest extends FunSuite with BeforeAndAfterAll {

  val log: Logger = Logger.getLogger("TestLogger")

  val spark: SparkSession = SparkSession.builder()
    .appName(config.getString("spark.appName"))
    .master(config.getString("spark.master"))
    .config("spark.sql.files.ignoreCorruptFiles", value = true)
    .getOrCreate()

  val externalSchema: Boolean = config.getString("schema.external_schema").toBoolean


  val dataset: String = """[
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_nifi_time":"1568037188486","serialField":"0","random_Field":"100","WordField":"qwe","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_nifi_time":"1568037188487","serialField":"1","random_Field":"-90","WordField":"rty","junkField":"132_.","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_nifi_time":"1568037188487","serialField":"2","random_Field":"50","WordField":"uio","junkField":"asd.cx","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_nifi_time":"1568037188487","serialField":"3","random_Field":"20","WordField":"GreenPeace","junkField":"XYZ","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_nifi_time":"1568037188487","serialField":"4","random_Field":"30","WordField":"fgh","junkField":"123_ASD","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_nifi_time":"1568037188487","serialField":"5","random_Field":"50","WordField":"jkl","junkField":"casd(@#)asd","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_nifi_time":"1568037188487","serialField":"6","random_Field":"60","WordField":"zxc","junkField":"QQQ.2","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_nifi_time":"1568037188487","serialField":"7","random_Field":"-100","WordField":"RUS","junkField":"00_3","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_nifi_time":"1568037188487","serialField":"8","random_Field":"0","WordField":"MMM","junkField":"112","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
                          |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_nifi_time":"1568037188487","serialField":"9","random_Field":"10","WordField":"USA","junkField":"word","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"}
                          |]"""
  val tmpDir: String =  "src/test/resources/temp"
  val test_index = s"test_index-${this.getClass.getSimpleName}"

  def jsonCompare(json1 : String,json2 : String): Boolean = {
    import spray.json._
    val j1 = json1.parseJson.sortedPrint
    val j2 = json2.parseJson.sortedPrint
    j1==j2
  }

  def execute(query: String): String = {
    val otlQuery = createQuery(query)
    val df = new Converter(otlQuery).run
    df.toJSON.collect().mkString("[\n",",\n","\n]")
  }

  def execute(query: String, tws: Int, twf: Int): String = {
    val otlQuery = createQuery(query, tws, twf)

    val df = new Converter(otlQuery).run
    df.toJSON.collect().mkString("[\n",",\n","\n]")
  }
  
  def execute(otlQuery : OTLQuery): String = {
    val df = new Converter(otlQuery).run
    df.toJSON.collect().mkString("[\n",",\n","\n]")
  }

  def execute(query: String, initDf: DataFrame): String = {
    val otlQuery = createQuery(query)
    val df = new Converter(otlQuery).setDF(initDf).run
    df.toJSON.collect().mkString("[\n",",\n","\n]")
  }

  def getFieldsUsed(query: String): String = {
    val otlQuery = OTLQuery(query)
    val fieldsUsed = new Converter(otlQuery).fieldsUsed.map(_.stripBackticks()).sorted
    fieldsUsed.mkString(", ")
  }

  def createQuery(command_otl: String, tws: Int = 0, twf: Int = 0): OTLQuery ={
    val otlQuery = new OTLQuery(
      id = 0,
      original_otl = s"search index=$test_index | $command_otl",
      service_otl = s""" | read {"$test_index": {"query": "", "tws": "$tws", "twf": "$twf"}} |  $command_otl """,
      tws = tws,
      twf = twf,
      cache_ttl = 0,
      indexes = Array(test_index),
      subsearches = Map(),
      username = "admin",
      field_extraction = false,
      preview = false
    )
    log.debug(s"otlQuery: $otlQuery.")
    otlQuery
  }

  def jsonToDf(json :String): DataFrame = {
    import spark.implicits._
    spark.read.json(Seq(json.stripMargin).toDS)
  }

  override def beforeAll() {
    val junkIndex = new Directory(new File(f"$tmpDir/indexes/$test_index"))
    if(junkIndex.exists && junkIndex.list.nonEmpty) junkIndex.deleteRecursively()
    val df = jsonToDf(dataset)
    val backetPath = f"$tmpDir/indexes/$test_index/bucket-0-${Int.MaxValue}-${System.currentTimeMillis / 1000}"
    df.write.parquet(backetPath)
    if(externalSchema)
        new PrintWriter(backetPath + "/all.schema") {
      write(df.schema.toDDL.replace(",","\n")); close()
    }
  }

  override def afterAll() {
    val indexDir = new Directory(new File(f"$tmpDir/indexes/$test_index"))
    indexDir.deleteRecursively()
    val indexesPath = new File(f"$tmpDir/indexes")
    if(new Directory(indexesPath).list.isEmpty) new Directory(indexesPath.getParentFile).deleteRecursively()
  }

  def writeTextFile(content : String, relativePath :String) : Unit={
    val lookupFile = tmpDir + "/" + relativePath
    val directory = new Directory(new File(lookupFile).getParentFile)
    directory.createDirectory()
    new PrintWriter(lookupFile) { write(content); close() }
  }
}
