package ot.dispatcher.plugins.externaldata.commands

import org.apache.spark.sql.{DataFrame, SaveMode}
import ot.dispatcher.plugins.externaldata.internals.ExternalFile
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.PluginUtils

/**
 * SMaLL command. It writes any file of compatible with Spark format.
 * @param sq [[SimpleQuery]] search query object.
 * @return [[DataFrame]]
 */
class WriteFile(sq: SimpleQuery, utils: PluginUtils) extends ExternalFile(sq, utils) {

  val mode: String = getKeyword("mode").getOrElse("overwrite")
  val numPartitions: Option[String] = getKeyword("numPartitions")
  val partitionBy: Option[String] = getKeyword("partitionBy")

  override def transform(_df: DataFrame): DataFrame = {

    val df = numPartitions match {
      case Some(np) => _df.repartition(np.toInt)
      case None => _df
    }

    val dfw = partitionBy match {
      case Some(pb) => df.write.partitionBy(pb.split(",").map(_.trim):_*)
      case None => df.write
    }

    dfw.format(format).mode(mode).option("header", header).save(absolutePath)
    df
  }

}
