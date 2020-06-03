package ot.scalaotl.config

import com.typesafe.config.Config
import ot.AppConfig.config


trait OTLConfig {
  val otlconfig: Config = config
}
