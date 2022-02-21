package ot.scalaotl
package commands

import org.apache.spark.sql.DataFrame
import ot.scalaotl.extensions.DataFrameExt._

/** =Abstract=
 * This class provides support of __'''append'''__ otl command.
 *
 * __'''append'''__ takes __'''subsearch'''__ (otl query in square brackets) as an argument
 *
 * and appends the results of its execution to the current results.
 *
 * =Usage example=
 * OTL:
 * {{{ | makeresults | eval a = 1 | eval b = 2 | eval c = 3
 * | append [makeresults | eval a = 4 | eval b = 5 | eval c = 6] | fields a,b,c}}}
 *
 * Result:
 * {{{+---+---+---+
 * |  a|  b|  c|
 * +---+---+---+
 * |  1|  2|  3|
 * |  4|  5|  6|
 * +---+---+---+}}}
 *
 * @constructor creates new instance of [[OTLAppend]]
 * @param sq [[SimpleQuery]]
 */
class OTLAppend(sq: SimpleQuery) extends OTLBaseCommand(sq) {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set("subsearch")
  val cache: Map[String, DataFrame] = sq.cache

  /**
   * Tries to get the value of the __'''subsearch'''__ keyword, otherwise it takes '''__nosubsearch__'''
   *
   * as the value of the __'''subsearch'''__ keyword.
   *
   * Then, using this key, the __dataframe__ is taken from __cache__, if there is no __dataframe__
   * corresponding to the value in cache, then the input __dataframe__ is returned.
   *
   * @param _df - input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return _df with __'''subsearch'''__ result appended
   *
   * @todo escape underline symbols in __nosubsearch__ word
   */
  override def transform(_df: DataFrame): DataFrame = {
    val subsearch: String = getKeyword("subsearch").getOrElse("__nosubsearch__")
    val jdf: DataFrame = cache.getOrElse(subsearch, return _df)
    _df.append(jdf)
  }
}
