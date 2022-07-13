package ot.scalaotl
package commands

import ot.scalaotl.parsers.ExpressionParser
import ot.scalaotl.extensions.ColumnExt._
import ot.scalaotl.extensions.StringExt._

import ot.dispatcher.sdk.core.CustomException.E00020

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.DataFrame

/** =Abstract=
 * This class provides support of __'''eval'''__ otl command.
 *
 * __'''eval'''__ evaluates the expression and places the resulting value in the search result field.
 *
 *  1. the result of eval is a new field
 *
 *  1. if the name of the resulting field is already in the search results,
 *     it will be overwritten by __eval__
 *
 *  1. if __eval__ evaluation fails, then the resulting field will be empty (filled with '''NULL''')
 *
 *  1. to refer to a field containing not only numbers and letters in the name (except _),
 *     you must surround it with quotation marks
 * {{{eval newField = "field-1" + 1 + field}}}
 *
 * __'''eval'''__ takes two required arguments:
 *  1. field name in which the result of the calculation will be placed
 *
 *  1. expression - combination of values, variables, operators, and functions that,
 *     when evaluated, will produce a final value
 *
 * =Operators=
 * {{{+  takes two numbers to add or two strings to concatenate}}}
 *
 * {{{-  takes two numbers to subtract}}}
 *
 * {{{*  takes two numbers to multiply}}}
 *
 * {{{/  takes two numbers to divide}}}
 *
 * {{{% takes two numbers to calculate the remainder of the division}}}
 *
 * {{{AND, OR - logical operators, accept logical values as input}}}
 *
 * {{{>, <, >=, <=, !=, =, == logical operators, take two numbers or two strings as input }}}
 *
 * =Functions=
 * ===Conditional and comparative functions===
 * {{{case(X,"Y",...), coalesce(X,...), if(X,Y,Z), like(TEXT, PATTERN), match(SUBJECT, "REGEX"),
 *nullif(X,Y), true()}}}
 *
 * ===Conversion functions===
 * {{{printf, tonumber(NUMSTR,BASE), tostring(X,Y)}}}
 *
 * ===Cryptographic functions===
 * {{{md5(X), sha1(X)}}}
 *
 * ===Date and time functions===
 * {{{now(), relative_time(X,Y), strptime(X,Y)}}}
 *
 * ===Information functions===
 * {{{isnotnull(X), isnull(X)}}}
 *
 * ===Math functions===
 * {{{abs(X), ceiling(X), ceil(X), exp(X), floor(X), ln(X), log(X,Y), pi(), pow(X,Y),
 *round(X,Y), sqrt(X), acos(X), asin(X), atan(X), atan2(Y, X), cos(X), hypot(X,Y), sin(X), tan(X)}}}
 *
 * ===Multivalue Conversion Functions===
 * {{{mvappend(X,...), mvcount(MVFIELD), mvdedup(X), mvfind(MVFIELD,"REGEX"),
 *mvindex(MVFIELD,STARTINDEX, ENDINDEX), mvjoin(MVFIELD,STR), mvrange(X,Y,Z),
 *mvsort(X), mvzip(X,Y,"Z"), split(X,"Y")}}}
 *
 * ===Statistical functions===
 * {{{max(X,...), min(X,...)}}}
 *
 * ===Text functions===
 * {{{len(X), lower(X), replace(X,Y,Z), substr(X,Y,Z), upper(X)}}}
 *
 * =Usage example=
 * ===Example 1===
 * OTL:
 * {{{| makeresults  | eval a = mvappend(1,2,3,4,5) | mvexpand a
 *| eval b = if( a % 2 == 0, "even", "odd")}}}
 * Result:
 * {{{+----------+---+----+
 *|     _time|  a|   b|
 *+----------+---+----+
 *|1645814700|  1| odd|
 *|1645814700|  2|even|
 *|1645814700|  3| odd|
 *|1645814700|  4|even|
 *|1645814700|  5| odd|
 *+----------+---+----+}}}
 * ===Example 2===
 * OTL: {{{| makeresults  | eval mark = mvappend(1,2,3,4,5) | mvexpand mark
 *| eval reaction = case( mark < 3, "OMG!", mark >= 3 AND mark < 5, "Not so bad",
 *mark > 4, "You are genius" )}}}
 * Result:
 * {{{+----------+----+--------------+
 *|     _time|mark|      reaction|
 *+----------+----+--------------+
 *|1645815334|   1|          OMG!|
 *|1645815334|   2|          OMG!|
 *|1645815334|   3|    Not so bad|
 *|1645815334|   4|    Not so bad|
 *|1645815334|   5|You are genius|
 *+----------+----+--------------+}}}
 */
class OTLEval(sq: SimpleQuery) extends OTLBaseCommand(sq) with ExpressionParser {
  val requiredKeywords = Set.empty[String]
  val optionalKeywords = Set.empty[String]

  /**
   * @return [[List]] fields used in expression [[org.apache.spark.sql.catalyst.expressions.Expression]]
   */
  override def getFieldsUsed: Return => List[String] = (ret: Return) => {
    ret.evals.flatMap {
      case x if x.expr.isEmpty => List.empty[String]
      case x => getFieldsFromExpression(expr(x.expr).expr, List()).map(_.stripBackticks().addSurroundedBackticks)
    }
  }

  /**
   * Checks if '''keywordsMap''' is empty
   *
   * @throws E00020 [[ot.dispatcher.sdk.core.CustomException.E00020]] if '''keywordsMap''' is empty
   */
  override def validateArgs(): Unit = {
    if (keywordsMap.isEmpty)
      throw E00020(sq.searchId, commandName)
  }

  /**
   * @param _df input __dataframe__, passed by the [[Converter]] when executing an OTL query
   * @return _df with evaluated '''evals'''
   */
  override def transform(_df: DataFrame): DataFrame = {
    val sch = _df.schema
    returns.evals.foldLeft(_df)((acc, eval) => eval match {
      case StatsEval(newfield, expression) => acc
        .withColumn(newfield.strip("\"").strip("\'"), expr(expression.replaceFirst("""`\Q""" + newfield + """\E` *=""", "")
          .withPeriodReplace()).withExtensions(sch))
      case _ => acc
    }
    )
  }
}
