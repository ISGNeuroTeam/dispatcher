package ot.scalaotl.extensions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.{DataSourceV2, SessionConfigSupport}

import java.util.regex.Pattern

object IsgDataSourceV2Utils extends Logging {
  def extractSessionConfigs(ds: DataSourceV2, conf: SQLConf): Map[String, String] = ds match {
    case cs: SessionConfigSupport =>
      val keyPrefix = cs.keyPrefix()
      require(keyPrefix != null, "The data source config key prefix can't be null.")

      val pattern = Pattern.compile(s"^spark\\.datasource\\.$keyPrefix\\.(.+)")

      conf.getAllConfs.flatMap { case (key, value) =>
        val m = pattern.matcher(key)
        if (m.matches() && m.groupCount() > 0) {
          Seq((m.group(1), value))
        } else {
          Seq.empty
        }
      }

    case _ => Map.empty
  }
}
