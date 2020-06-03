package ot.scalaotl

import org.apache.spark.sql.SparkSession

trait OTLSparkSession {
  lazy val spark: SparkSession = SparkSession.getActiveSession match {
    case Some(s) => s
    case _       => SparkSession.builder.master("local[2]").appName("scalaotl").getOrCreate()
  }
}