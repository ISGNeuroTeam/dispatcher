package ot.dispatcher.MaintenanceFunctions

import org.apache.spark.sql.SparkSession
import ot.scalaotl.config.OTLIndexes
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.time.Instant

import org.apache.log4j.{Level, Logger}
import ot.AppConfig.getLogLevel

import scala.concurrent.{Await, ExecutionContext, Future}
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object FilterBloomCalculationProcess extends OTLIndexes {

  val log: Logger = Logger.getLogger("FBCPLogger")
  log.setLevel(Level.toLevel(getLogLevel(otlconfig,"fbcp")))
  var timeFutureStarted: Long = 0

  def calculate(systemMaintenanceArgs: Map[String, Any]): Unit = {

    val timeNow = Instant.now().toEpochMilli / 1000

    if (timeNow - timeFutureStarted > 1800) {

      val future = Future({

        val spark: SparkSession = systemMaintenanceArgs("sparkSession").asInstanceOf[SparkSession]
        val currentfs: FileSystem = {
          val conf = new Configuration()
          conf.set("fs.defaultFS", fsdisk)
          FileSystem.get(conf)
        }

        val indexes = currentfs.listStatus(new Path(indexPathDisk))
        import spark.implicits._
        for (index <- indexes if index isDirectory) {
          val dirs = currentfs.listStatus(index.getPath)
          for (d <- dirs if d isDirectory) {
            val subpath = f"${d.getPath}/bloom".substring(5)
            try {
              if (new java.io.File(subpath).exists == false) {
                val df = spark.read.parquet(f"${d.getPath}")
                val df2 = df.select("_raw").rdd.flatMap(f => f.getString(0).split(" ")).distinct().toDF()
                val bloom = df2.stat.bloomFilter($"value", 10000000, 0.01)
                val fos = new FileOutputStream(subpath)
                val target = new BufferedOutputStream(fos)
                bloom.writeTo(target)
                target.flush()
                target.close()
                fos.close()
              }
            } 
            catch {
              case _: Throwable => log.warn(s"Exception from parquet $subpath")

            }
          }
        }

      })

      timeFutureStarted = timeNow

      future.onComplete {
        case Success(state) => log.debug(s"Finished with state: $state.")
        case Failure(error) => log.error(s"Finished with error: ${error.getMessage}")
      }

      Await.ready(future, Duration("1800 sec"))

    }

  }

}
