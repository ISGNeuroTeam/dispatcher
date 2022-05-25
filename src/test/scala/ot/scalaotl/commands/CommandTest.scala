package ot.scalaotl.commands

import java.io.{File, PrintWriter}
import java.util.UUID

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types.{NullType, StringType, ArrayType, StructType, StructField, LongType}
import org.scalatest.{Assertion, BeforeAndAfterAll, FunSuite}
import ot.AppConfig.{config, getLogLevel}
import scala.reflect.{ClassTag, classTag}
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


  val dataset: String =
  """[
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
  val dataDir: String =  "src/test/resources/data"
  val tmpDir: String =  "src/test/resources/temp"
  val test_index = s"test_index-${this.getClass.getSimpleName}"

  def jsonCompare(json1 : String,json2 : String): Boolean = {
    import spray.json._
    val j1 = json1.parseJson.sortedPrint
    val j2 = json2.parseJson.sortedPrint
    j1==j2
  }


  def countCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      F.count(F.when(F.col(c).isNull,c)).alias(c)
    })
  }

  def execute(query: String): String = {
    val otlQuery = createQuery(query)
    val df = new Converter(otlQuery).run
    df.toJSON.collect().mkString("[\n", ",\n", "\n]")
  }


  def execute(query: String, tws: Int, twf: Int): String = {
    val otlQuery = createQuery(query, tws, twf)
    val df = new Converter(otlQuery).run
    df.toJSON.collect().mkString("[\n", ",\n", "\n]")
  }
  
  def execute(otlQuery : OTLQuery): String = {
    val df = new Converter(otlQuery).run
    df.printSchema()
    df.toJSON.collect().mkString("[\n", ",\n", "\n]")
  }

  def execute(query: String, initDf: DataFrame): String = {
    val otlQuery = createQuery(query)
    val df = new Converter(otlQuery).setDF(initDf).run
    df.toJSON.collect().mkString("[\n", ",\n", "\n]")
  }

  def getFieldsUsed(query: String): String = {
    val otlQuery = OTLQuery(query)
    val fieldsUsed = new Converter(otlQuery).fieldsUsed.map(_.stripBackticks()).sorted
    fieldsUsed.mkString(", ")
  }

  def createQuery(command_otl: String, tws: Int = 0, twf: Int = 0): OTLQuery ={
    val cmd = if (this.getClass.getSimpleName.contains("FullRead")) "otstats" else "read"
    val otlQuery = new OTLQuery(
      id = 0,
      original_otl = s"search index=$test_index | $command_otl",
      service_otl = s""" | $cmd {"$test_index": {"query": "", "tws": "$tws", "twf": "$twf"}} |  $command_otl """,
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


  /** This code is executed before running the OTL-command
   * Step 1 Check existing of test index
   * Step2 If index exists, it is removed
   * Step3 If an external data schema is used, it will be written next to the parquet
   */
  override def beforeAll() {
    val junkIndex = new Directory(new File(f"$tmpDir/indexes/$test_index"))
    if (junkIndex.exists && junkIndex.list.nonEmpty) junkIndex.deleteRecursively()
    var df = spark.emptyDataFrame
    if (this.getClass.getSimpleName.contains("FullRead") || this.getClass.getSimpleName.contains("RawRead")){
      for(ind <- 1 to 2){
        df = spark.read.options(Map("inferSchema"->"true", "delimiter"->",", "header"->"true"))
          .csv(f"$dataDir/sensors-$ind.csv")
          .withColumn("index", F.lit(f"$test_index-$ind"))
          .withColumn("_time", col("_time").cast(LongType))
        val time_min_max = df.agg(min("_time"), max("_time")).head()
        val time_step = (time_min_max.getLong(1) - time_min_max.getLong(0));
        val bucket_period = 3600*24*30
        val buckets_cnt = math.floor(time_step / bucket_period).toInt
        for (i <- 0 to buckets_cnt) {
          val lowerBound = i * bucket_period + time_min_max.getLong(0);
          val upperBound = (i + 1) * bucket_period + time_min_max.getLong(0);
          val bucketPath = f"$tmpDir/indexes/$test_index-$ind/bucket-${lowerBound}-${upperBound}-${System.currentTimeMillis / 1000}";
          val df_bucket = df.filter(F.col("_time").between(lowerBound, upperBound));
          df_bucket.write.parquet(bucketPath)
          //        println(bucketPath)
          if (externalSchema)
            new PrintWriter(bucketPath + "/all.schema") {
              write(df_bucket.schema.toDDL.replace(",", "\n"));
              close()
            }
        }
      }
    }
    else{
      df = jsonToDf(dataset)
      val bucketPath = f"$tmpDir/indexes/$test_index/bucket-0-${Int.MaxValue}-${System.currentTimeMillis / 1000}"
      df.write.parquet(bucketPath)
      if(externalSchema)
        new PrintWriter(bucketPath + "/all.schema") {
          write(df.schema.toDDL.replace(",","\n")); close()
        }
    }
  }

  /** This code is executed after running the OTL-command
   * Step1 Recursively delete created indexes
   * Step2 If indexes directory is empty then delete it
   */
  override def afterAll() {
    if (this.getClass.getSimpleName.contains("FullRead") || this.getClass.getSimpleName.contains("RawRead")){
      for(ind <- 1 to 2) {
        val indexDir = new Directory(new File(f"$tmpDir/indexes/$test_index-$ind"))
        indexDir.deleteRecursively()
        val indexesPath = new File(f"$tmpDir/indexes")
        if (new Directory(indexesPath).list.isEmpty) new Directory(indexesPath.getParentFile).deleteRecursively()
      }
    }
      else{
      val indexDir = new Directory(new File(f"$tmpDir/indexes/$test_index"))
      indexDir.deleteRecursively()
      val indexesPath = new File(f"$tmpDir/indexes")
      if(new Directory(indexesPath).list.isEmpty) new Directory(indexesPath.getParentFile).deleteRecursively()
    }
  }

  def setNullableStateOfColumn(df: DataFrame, col_name: String, nullable: Boolean) : DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField( c, t, _, m) if c.equals(col_name) => StructField( c, t, nullable = nullable, m)
      case y: StructField => y
    })
    df.sqlContext.createDataFrame( df.rdd, newSchema )
  }

  def writeTextFile(content : String, relativePath :String) : Unit={
    spark.stop()
    val lookupFile = tmpDir + "/" + relativePath
    val directory = new Directory(new File(lookupFile).getParentFile)
    directory.createDirectory()
    new PrintWriter(lookupFile) { write(content); close() }

  }

  def compareDataFrames(df_actual: DataFrame, df_expected: DataFrame): Assertion={
    assert(
      df_actual.schema == df_expected.schema,
      "DataFrames schemas should be equal"
    )

    assert(
      df_actual.count() == df_expected.count(),
      "DataFrames sizes should be equal"
    )

    val diff = df_actual.except(df_expected).union(df_expected.except(df_actual))

    assert(
      diff.isEmpty,
      s"DataFrames are different. Difference is:\n${diff.collect().mkString("\n")}"
    )

  }




}
