package com.isgneuro.otl.commands

import com.isgneuro.sparkexecenv.{BaseCommand, CommandExecutor, CommandsProvider}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.scalatest.{Assertion, BeforeAndAfterAll, FunSuite}
import ot.AppConfig
import ot.AppConfig.config
import play.api.libs.json.{JsValue, Json}

import java.io.{File, PrintWriter}
import java.net.URI
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory

class Command2Test extends FunSuite with BeforeAndAfterAll {
  val log: Logger = Logger.getLogger("TestLogger")

  val spark: SparkSession = SparkSession.builder()
    .appName(config.getString("spark.appName"))
    .master(config.getString("spark.master"))
    .config("spark.sql.files.ignoreCorruptFiles", value = true)
    .getOrCreate()
  val sparkContext = spark.sparkContext
  sparkContext.setCheckpointDir("src/test/checkpoints")
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
      |{"_time":1568026476855,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_nifi_time":"1568037188487","serialField":"1","random_Field":"-90","WordField":"rty","junkField":"132_.","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476856,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_nifi_time":"1568037188487","serialField":"2","random_Field":"50","WordField":"uio","junkField":"asd.cx","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476857,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_nifi_time":"1568037188487","serialField":"3","random_Field":"20","WordField":"GreenPeace","junkField":"XYZ","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476858,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_nifi_time":"1568037188487","serialField":"4","random_Field":"30","WordField":"fgh","junkField":"123_ASD","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476859,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_nifi_time":"1568037188487","serialField":"5","random_Field":"50","WordField":"jkl","junkField":"casd(@#)asd","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476860,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_nifi_time":"1568037188487","serialField":"6","random_Field":"60","WordField":"zxc","junkField":"QQQ.2","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476861,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_nifi_time":"1568037188487","serialField":"7","random_Field":"-100","WordField":"RUS","junkField":"00_3","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476862,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_nifi_time":"1568037188487","serialField":"8","random_Field":"0","WordField":"MMM","junkField":"112","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476863,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_nifi_time":"1568037188487","serialField":"9","random_Field":"10","WordField":"USA","junkField":"word","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"}
      |]"""

  val tmpDir: String = "src/test/resources/temp"
  val test_index = s"test_index-${this.getClass.getSimpleName}"
  val stfeRawSeparators: Array[String] = Array("json", ": ", "= ", " :", " =", " : ", " = ", ":", "=")

  val datasetSchema: StructType = StructType(Array(
    StructField("WordField", StringType, nullable = true),
    StructField("_meta", StringType, nullable = true),
    StructField("_nifi_time", StringType, nullable = true),
    StructField("_nifi_time_out", StringType, nullable = true),
    StructField("_raw", StringType, nullable = true),
    StructField("_subsecond", StringType, nullable = true),
    StructField("_time", LongType, nullable = true),
    StructField("host", StringType, nullable = true),
    StructField("index", StringType, nullable = true),
    StructField("junkField", StringType, nullable = true),
    StructField("random_Field", StringType, nullable = true),
    StructField("serialField", StringType, nullable = true),
    StructField("source", StringType, nullable = true),
    StructField("sourcetype", StringType, nullable = true),
    StructField("timestamp", StringType, nullable = true)
  ))

  val readingDatasetSchema: StructType = StructType(Array(
    StructField("_time", LongType, nullable = true),
    StructField("floor", IntegerType, nullable = true),
    StructField("room", IntegerType, nullable = true),
    StructField("ip", StringType, nullable = true),
    StructField("id", StringType, nullable = true),
    StructField("type", StringType, nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("metric_name", StringType, nullable = true),
    StructField("metric_long_name", StringType, nullable = true),
    StructField("value", DoubleType, nullable = true),
    StructField("index", StringType, nullable = true),
    StructField("text", StringType, nullable = true),
    StructField("text[1].val", StringType, nullable = true),
    StructField("text[2].val", StringType, nullable = true),
    StructField("num", IntegerType, nullable = true),
    StructField("num[1].val", IntegerType, nullable = true),
    StructField("num[2].val", IntegerType, nullable = true),
    StructField("listField", StringType, nullable = true),
    StructField("nestedField", StringType, nullable = true),
    StructField("_raw", StringType, nullable = true)))

  // Example timestamps
  val startTime = 1649145660
  val finishTime = 1663228860

  var commandClasses:  Map[String, Class[_ <: BaseCommand]] = Map()

  override def beforeAll(): Unit = {
    cleanIndexFiles(tmpDir)
    val log: Logger = Logger.getLogger("test")
    val kafkaExists: Boolean = config.getBoolean("kafka.computing_node_mode_enabled")
    val commandsProvider: Option[CommandsProvider] = if (kafkaExists) {
      Some(new CommandsProvider(config.getString("usercommands.directory"), log))
    } else {
      None
    }
    commandClasses = if (kafkaExists) {commandsProvider.get.commandClasses} else {Map()}
    if (List("SearchCommand", "OtstatsCommand").exists(this.getClass.getName.contains(_))) {
      val df = spark.read.options(
          Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true", "quote" -> "\"", "escape" -> "\"")
        )
        .csv(this.getClass.getClassLoader.getResource("data/sensors.csv").getPath)
        .withColumn("index", F.lit(s"$test_index-0"))
        .withColumn("_time", F.col("_time").cast(LongType))
      val time_min_max = df.agg(F.min("_time"), F.max("_time")).head()
      val time_step = time_min_max.getLong(1) - time_min_max.getLong(0)
      val bucket_period = 3600 * 24 * 30
      val buckets_cnt = math.floor(time_step / bucket_period).toInt
      for (i <- stfeRawSeparators.indices) {
        val df_new = if (i == 0) df
        else df.withColumn("index", F.lit(s"$test_index-$i"))
          .withColumn("_raw", F.ltrim(F.col("_raw"), "{"))
          .withColumn("_raw", F.rtrim(F.col("_raw"), "}"))
          .withColumn("_raw", F.regexp_replace(F.col("_raw"), F.lit(": "), F.lit(stfeRawSeparators(i))))
        for (j <- 0 to buckets_cnt) {
          val lowerBound = j * bucket_period + time_min_max.getLong(0)
          val upperBound = (j + 1) * bucket_period + time_min_max.getLong(0)
          val bucketPath = s"$tmpDir/indexes/$test_index-$i/bucket-$lowerBound-$upperBound-${System.currentTimeMillis / 1000}"
          val df_bucket = df_new.filter(F.col("_time").between(lowerBound, upperBound))
          df_bucket.write.parquet(bucketPath)
          if (externalSchema)
            new PrintWriter(bucketPath + "/all.schema") {
              write(df_bucket.schema.toDDL.replace(",", "\n"))
              close()
            }
        }
      }
    } else {
      val df = jsonToDf(dataset)
      val bucketPath = s"$tmpDir/indexes/$test_index/bucket-0-${Int.MaxValue}-${System.currentTimeMillis / 1000}"
      df.write.parquet(bucketPath)
      if (externalSchema)
        new PrintWriter(bucketPath + "/all.schema") {
          write(df.schema.toDDL.replace(",", "\n"))
          close()
        }
    }
  }

  def cleanIndexFiles(path: String): AnyVal = {
    val indexDir = new Directory(new File(path))
    if (indexDir.exists) indexDir.deleteRecursively()
  }

  def jsonToDf(json: String): DataFrame = {
    import spark.implicits._
    spark.read.json(Seq(json.stripMargin).toDS)
  }

  def executeQuery(query: String, readCommand: String = "search"): String = {
    val resDf = runQuery(query, readCommand)
    resDf.toJSON.collect().mkString("[\n", ",\n", "\n]")
  }

  def runQuery(query: String, readCommand: String = "search"): DataFrame = {
    val fullQuery = createQuery(query, readCommand)
    val cmdJson: JsValue = Json.parse(fullQuery)
    val commandsExecutor = new CommandExecutor(AppConfig.config, commandClasses, null)
    commandsExecutor.execute("1", cmdJson.as[List[JsValue]])
  }

  def executeFull(query: String): DataFrame = {
    val cmdJson: JsValue = Json.parse(query)
    val commandsExecutor = new CommandExecutor(AppConfig.config, commandClasses, null)
    commandsExecutor.execute("1", cmdJson.as[List[JsValue]])
  }

  def createQuery(otlCommand: String, readCommand: String = "search",
                  index: String = test_index, startTime: Int = 0, finishTime: Int = 0): String = {
    val readQueryPart = readCommand match {
      case "search" =>
        s"""{
           |"name": "$readCommand",
           |"arguments": {
           | "index": [
           | {
           |          "key": "index",
           |          "type": "string",
           |          "value": "$index",
           |          "arg_type": "arg",
           |          "group_by": [],
           |          "named_as": ""
           | }
           | ],
           | "earliest": [
           | {
           |          "key": "earliest",
           |          "type": "integer",
           |          "value": $startTime,
           |          "arg_type": "arg",
           |          "group_by": [],
           |          "named_as": ""
           | }
           | ],
           | "latest": [
           | {
           |          "key": "latest",
           |          "type": "integer",
           |          "value": $finishTime,
           |          "arg_type": "arg",
           |          "group_by": [],
           |          "named_as": ""
           | }
           | ]
           |}
           |}""".stripMargin
      //case "otstats" =>
      case _ => ""
    }
    "[" + readQueryPart + {if(otlCommand.nonEmpty) "," else ""} + otlCommand + "]"
  }

  def readIndexDF(index: String, schema: StructType = datasetSchema): DataFrame = {
    val filenames = ListBuffer[String]()
    try {
      val status = fs_disk.listStatus(new Path(s"$tmpDir/indexes/$index"))
      status.foreach(x => filenames += s"$tmpDir/indexes/$index/${x.getPath.getName}")
    }
    catch {
      case e: Exception => log.debug(e);
    }
    spark.read.options(Map("recursiveFileLookup" -> "true")).schema(schema)
      .parquet(filenames.seq: _*)
      .withColumn("index", F.lit(index))
  }

  def jsonCompare(json1: String, json2: String): Boolean = {
    import spray.json._
    val j1 = json1.parseJson.sortedPrint
    val j2 = json2.parseJson.sortedPrint
    j1 == j2
  }

  override def afterAll(): Unit = {
    cleanIndexFiles(tmpDir)
    cleanCheckpointsDirectory()
  }

  def cleanCheckpointsDirectory(): Unit = {
    sparkContext.getCheckpointDir match {
      case Some(dir) =>
        val fs = org.apache.hadoop.fs.FileSystem.get(new URI(dir), sparkContext.hadoopConfiguration)
        fs.delete(new Path(dir), true)
      case None =>
    }
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
      assert(true)
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
}
