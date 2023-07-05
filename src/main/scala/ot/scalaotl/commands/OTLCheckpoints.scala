package ot.scalaotl.commands

import org.apache.spark.sql.DataFrame
import ot.AppConfig
import ot.dispatcher.sdk.core.CustomException.E00012
import ot.scalaotl.SimpleQuery
import ot.scalaotl.extensions.StringExt._

/** =Abstract=
 * This class provides support of __'''checkpoints'''__ otl command.
 *
 * __'''checkpoints'''__ manages the possibility of checkpointing technology applying to improve query performance and fault tolerance.
 * This command controls a setting of the application by on|off principle and does not change the dataframe.
 *
 * __'''checkpoints'''__ takes one required argument:
 *
 *    1.  '''managing_word''' - a word from a set of 2 values (on/off),
 *    by which a decision is made regarding the operation of the checkpointing technology during the execution of queries.
 *
 *    Allowed values: '''on''', '''off'''.
 *
 * =Usage examples=
 * * OTL 1:
 * {{{| makeresults | checkpoints on}}}
 * Result: the checkpointing technology will be work in queries with large count of commands, the calculation will not be heavy due to the distribution of the load.
 *
 *
 * * OTL 2:
 * {{{| makeresults | checkpoints off}}}
 * Result: the checkpointing technology will not be work.

 * @constructor creates new instance of [[OTLCheckpoints]]
 * @param sq [[SimpleQuery]]
 */

class OTLCheckpoints(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  override val requiredKeywords =  Set.empty[String]
  override val optionalKeywords =  Set.empty[String]

  override def transform(_df: DataFrame): DataFrame = {
    val command = if(returns.flatFields.nonEmpty)
      returns.flatFields.head
    else
      throw E00012(sq.searchId, commandname, "managing_word")
    if (command.stripBackticks() == "on" && !AppConfig.withCheckpoints)
      AppConfig.withCheckpoints = true
    else if (command.stripBackticks() == "off" && AppConfig.withCheckpoints)
      AppConfig.withCheckpoints = false
    _df
  }
}