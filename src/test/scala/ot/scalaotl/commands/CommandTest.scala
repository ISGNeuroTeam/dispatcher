package ot.scalaotl.commands

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{File, PrintWriter}
import java.util.UUID
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, NullType, StringType, StructField, StructType}
import org.scalatest.{Assertion, BeforeAndAfterAll, FunSuite}
import ot.AppConfig.{config, getLogLevel}

import scala.reflect.{ClassTag, classTag}
import ot.dispatcher.OTLQuery
import ot.scalaotl.Converter
import ot.scalaotl.extensions.StringExt._

import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory

abstract class CommandTest extends FunSuite with BeforeAndAfterAll {

  val log: Logger = Logger.getLogger("TestLogger")

  val spark: SparkSession = SparkSession.builder()
    .appName(config.getString("spark.appName"))
    .master(config.getString("spark.master"))
    .config("spark.sql.files.ignoreCorruptFiles", value = true)
    .getOrCreate()

  val externalSchema: Boolean = config.getString("schema.external_schema").toBoolean
  val fsdisk: String = config.getString("indexes.fs_disk")
  val fs_disk: FileSystem = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", fsdisk)
    FileSystem.get(conf)
  }

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
  val stfeRawSeparators: Array[String] = Array("json", ": ", "= ", " :", " =", " : ", " = ", ":", "=")
  // example timestamps for tws twf
  val start_time = 1649145660
  val finish_time = 1663228860

  val datasetSchema: StructType =StructType(Array(
    StructField("WordField",StringType,nullable = true),
    StructField("_meta",StringType,nullable = true),
    StructField("_nifi_time",StringType,nullable = true),
    StructField("_nifi_time_out",StringType,nullable = true),
    StructField("_raw",StringType,nullable = true),
    StructField("_subsecond",StringType,nullable = true),
    StructField("_time",LongType,nullable = true),
    StructField("host",StringType,nullable = true),
    StructField("index",StringType,nullable = true),
    StructField("junkField",StringType,nullable = true),
    StructField("random_Field",StringType,nullable = true),
    StructField("serialField",StringType,nullable = true),
    StructField("source",StringType,nullable = true),
    StructField("sourcetype",StringType,nullable = true),
    StructField("timestamp",StringType,nullable = true)
  ))

  val readingDatasetSchema: StructType =StructType(Array(
    StructField("_time",LongType,nullable = true),
    StructField("floor",IntegerType,nullable = true),
    StructField("room",IntegerType,nullable = true),
    StructField("ip",StringType,nullable = true),
    StructField("id",StringType,nullable = true),
    StructField("type",StringType,nullable = true),
    StructField("description",StringType,nullable = true),
    StructField("metric_name",StringType,nullable = true),
    StructField("metric_long_name",StringType,nullable = true),
    StructField("value",DoubleType,nullable = true),
    StructField("index",StringType,nullable = true),
    StructField("text",StringType,nullable = true),
    StructField("text[1].val",StringType,nullable = true),
    StructField("text[2].val",StringType,nullable = true),
    StructField("num",IntegerType,nullable = true),
    StructField("num[1].val",IntegerType,nullable = true),
    StructField("num[2].val",IntegerType,nullable = true),
    StructField("listField",StringType,nullable = true),
    StructField("nestedField",StringType,nullable = true),
    StructField("_raw",StringType,nullable = true)))

  def createFullQuery(command_original_otl: String, service_original_otl: String,
                      index: String, tws: Int = 0, twf: Int = 0, fe: Boolean = false): OTLQuery ={
    val otlQuery = new OTLQuery(
      id = 0,
      original_otl = command_original_otl,
      service_otl = service_original_otl,
      tws = tws,
      twf = twf,
      cache_ttl = 0,
      indexes = Array(index),
      subsearches = Map(),
      username = "admin",
      field_extraction = fe,
      preview = false
    )
    log.debug(s"otlQuery: $otlQuery.")
    otlQuery
  }

  def createQuery(command_otl: String, read_cmd: String = "read",
                  index: String = test_index, tws: Int = 0, twf: Int = 0): OTLQuery ={
    createFullQuery(
      s"search index=$index| $command_otl",
      s""" | $read_cmd {"$index": {"query": "", "tws": "$tws", "twf": "$twf"}} |  $command_otl """,
      test_index, tws, twf)
  }

  def createQuery(command_otl: String): OTLQuery ={
    createQuery(command_otl, "read", test_index)
  }

  def readIndexDF(index : String, schema: StructType = datasetSchema): DataFrame ={
    val filenames = ListBuffer[String]()
    try {
      val status = fs_disk.listStatus(new Path(s"$tmpDir/indexes/$index"))
      status.foreach(x => filenames += s"$tmpDir/indexes/$index/${x.getPath.getName}")
    }
    catch { case e: Exception => log.debug(e);}
    spark.read.options(Map("recursiveFileLookup"->"true")).schema(schema)
      .parquet(filenames.seq: _*)
      .withColumn("index", F.lit(index))
  }

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


  def execute(otlQuery : OTLQuery): String = {
    val df = new Converter(otlQuery).run
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



  def jsonToDf(json :String): DataFrame = {
    import spark.implicits._
    spark.read.json(Seq(json.stripMargin).toDS)
  }


  /** This code is executed before running the OTL-command
   * Step 1 Check existing of test index
   * Step2 If index exists, it is removed
   * Step3 If an external data schema is used, it will be written next to the parquet
   */
  override def beforeAll(): Unit = {
    val indexDir = new Directory(new File(f"$tmpDir/indexes"))
    if (indexDir.exists && indexDir.list.nonEmpty) indexDir.deleteRecursively()
    var df = spark.emptyDataFrame
    if (this.getClass.getSimpleName.contains("FullRead") || this.getClass.getSimpleName.contains("RawRead")){

        df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true", "quote"->"\"", "escape"->"\""))
          .csv(f"$dataDir/sensors.csv")
          .withColumn("index", F.lit(f"$test_index-0"))
          .withColumn("_time", col("_time").cast(LongType))
      val time_min_max = df.agg(min("_time"), max("_time")).head()
        val time_step = time_min_max.getLong(1) - time_min_max.getLong(0)
        val bucket_period = 3600 * 24 * 30
        val buckets_cnt = math.floor(time_step / bucket_period).toInt
        for(i <- stfeRawSeparators.indices){
          val df_new = if (i == 0) df else df.withColumn("index", F.lit(f"$test_index-$i"))
            .withColumn("_raw", F.ltrim(F.col("_raw"), "{"))
            .withColumn("_raw", F.rtrim(F.col("_raw"), "}"))
              .withColumn("_raw", F.regexp_replace(F.col("_raw"), F.lit(": "), F.lit(stfeRawSeparators(i))))
          for (j <- 0 to buckets_cnt) {
            val lowerBound = j * bucket_period + time_min_max.getLong(0)
            val upperBound = (j + 1) * bucket_period + time_min_max.getLong(0)
            val bucketPath = f"$tmpDir/indexes/$test_index-$i/bucket-$lowerBound-$upperBound-${System.currentTimeMillis / 1000}"
            val df_bucket = df_new.filter(F.col("_time").between(lowerBound, upperBound))


            df_bucket.write.parquet(bucketPath)
            if (externalSchema)
              new PrintWriter(bucketPath + "/all.schema") {
                write(df_bucket.schema.toDDL.replace(",", "\n"))
                close()
              }
          }
        }
    }
    else {
      df = jsonToDf(dataset)
      val bucketPath = f"$tmpDir/indexes/$test_index/bucket-0-${Int.MaxValue}-${System.currentTimeMillis / 1000}"
      df.write.parquet(bucketPath)
      if (externalSchema)
        new PrintWriter(bucketPath + "/all.schema") {
          write(df.schema.toDDL.replace(",", "\n"))
          close()
        }
    }
  }

  /** This code is executed after running the OTL-command
   * Step1 Recursively delete created indexes
   * Step2 If indexes directory is empty then delete it
   */
  override def afterAll(): Unit = {
    val indexDir = new Directory(new File(f"$tmpDir/indexes"))
    indexDir.deleteRecursively()
  }

  def setNullableStateOfColumn(df: DataFrame, column: String, nullable: Boolean) : DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField( c, t, _, m) if c.equals(column) => StructField( c, t, nullable = nullable, m)
      case y: StructField => y
    })
    df.sqlContext.createDataFrame( df.rdd, newSchema )
  }

  def setNullableStateOfColumn(df: DataFrame, columns: List[String], nullable: Boolean) : DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField( c, t, _, m) if columns.contains(c) => StructField( c, t, nullable = nullable, m)
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

    if (df_actual.isEmpty && df_expected.isEmpty)
      assert(condition = true)
    else {
      val diff = df_actual.except(df_expected).union(df_expected.except(df_actual))
      assert(
        diff.isEmpty,
        s"DataFrames are different. " +
          s"\n Counts actual ${df_actual.count()} / expected ${df_expected.count()} / diff ${diff.count()} " +
          s"\n Difference is:\n${diff.collect().mkString("\n")}"
      )
    }
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }




}
