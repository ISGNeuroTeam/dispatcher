package ot.dispatcher.sdk.proxy

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginCommand
import ot.dispatcher.sdk.core.SimpleQuery
import ot.scalaotl.commands.OTLBaseCommand

/**
 * @param sq [[SimpleQuery]] - query passed to command
 */
class PluginProxyCommand(command: PluginCommand, sq: ot.scalaotl.SimpleQuery) extends OTLBaseCommand(sq, command.separators) {

  override val requiredKeywords: Set[String] = Set()
  override val optionalKeywords: Set[String] = Set()

  def transform(_df: DataFrame): DataFrame = command.transform(_df)

  override def fieldsGenerated: List[String] = command.fieldsGenerated

  override def log: Logger = command.log

  override def loggerName: String = command.loggerName

  override def logLevel: String = command.logLevel

  override def commandname: String = command.commandname

  override def fieldsUsed: List[String] = command.fieldsUsed
}
