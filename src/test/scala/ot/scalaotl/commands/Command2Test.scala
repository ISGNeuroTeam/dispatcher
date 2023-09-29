package ot.scalaotl.commands

import com.isgneuro.sparkexecenv.{BaseCommand, CommandExecutor, CommandsProvider}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import ot.AppConfig
import ot.AppConfig.config
import play.api.libs.json.{JsValue, Json}

class Command2Test extends FunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder()
    .appName(config.getString("spark.appName"))
    .master(config.getString("spark.master"))
    .config("spark.sql.files.ignoreCorruptFiles", value = true)
    .getOrCreate()
  val sparkContext = spark.sparkContext
  val ddataset = Seq(
    (0, 100, "qwe", "q2W"),
    (1, -90, "rty", "132_."),
    (2, 50, "uio", "asd.cx"),
    (3, 20, "GreenPeace", "XYZ"),
    (4, 30, "fgh", "123_ASD"),
    (5, 50, "jkl", "casd(@#)asd"),
    (6, 60, "zxc", "QQQ.2"),
    (7, -100, "RUS", "00_3"),
    (8, 0, "MMM", "112"),
    (9, 10, "USA", "word")
  )

  var commandClasses:  Map[String, Class[_ <: BaseCommand]] = Map()

  override def beforeAll(): Unit = {
    val log: Logger = Logger.getLogger("test")
    val kafkaExists: Boolean = config.getBoolean("kafka.computing_node_mode_enabled")
    val commandsProvider: Option[CommandsProvider] = if (kafkaExists) {
      Some(new CommandsProvider(config.getString("usercommands.directory"), log))
    } else {
      None
    }
    commandClasses = if (kafkaExists) {commandsProvider.get.commandClasses} else {Map()}
  }

  def execute(query: String): String = {
    val cmdJson: JsValue = Json.parse(query)
    val commandsExecutor = new CommandExecutor(AppConfig.config, commandClasses, null)
    val resDf = commandsExecutor.execute("1", cmdJson.as[List[JsValue]])
    resDf.toJSON.collect().mkString("[\n", ",\n", "\n]")
  }
}
