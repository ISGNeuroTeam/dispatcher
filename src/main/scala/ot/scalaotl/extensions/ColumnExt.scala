package ot.scalaotl
package extensions

import ot.scalaotl.static.EvalFunctions
import ot.scalaotl.extensions.StringExt._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DataType, NumericType, StructType}
import org.apache.spark.sql.catalyst.expressions.{Alias, CaseWhen, CreateArray, ExprId, Expression, Literal}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.aggregate._

object ColumnExt {

  /**
   * Change function `Add` to function `concat` if function contains non-numeric children
   *
   * @param ex [[Expression]] - Catalyst expression to check
   * @param sch [[DataType]] - scheme of handled dataframe. Required for column types check
   * @return [[Expression]] - modified Catalyst expression
   */
  def changeAddToConcat(ex: Expression, sch: StructType): Expression = {
    // Get list of types used in expression
    def getTypes(expr: Expression): List[DataType] = expr.nodeName match {
      case "Literal"             => List(expr.dataType)
      case "UnresolvedAttribute" => List(sch(expr.toString.replaceAll("'", "").replaceAll("`", "")).dataType)
      case "UnresolvedFunction" => List[DataType]()
      case _                     => expr.children.map(x => getTypes(x)).toList.flatten
    }

    if (ex.nodeName == "Add") {
      val types = ex.children.map(x => {
        val types = getTypes(x)
        types
      })
      if (types.flatten.forall(_.isInstanceOf[NumericType])) ex else {
        UnresolvedFunction("concat", ex.children.map(changeAddToConcat(_, sch)), false)
      }
    } else ex.mapChildren(changeAddToConcat(_, sch))
  }

  /**
   * Replaces 'max' and 'min' functions to 'array_max' and 'array_min' respectively.
   * ! Must be applied before wrapping children to array (see below)
   * 
   * @param ex [[Expression]] Expression to modify
   * @return [[Expression]] Modified expression
   */
  def replaceMaxMin(ex: Expression): Expression = {
    val newEx = if ((ex.prettyName == "max") || (ex.prettyName == "min")) {
      UnresolvedFunction(s"array_${ex.prettyName}", ex.children, false)
    } else ex
    newEx.mapChildren(replaceMaxMin)
  }

  /**
   * If function might have variable number of arguments, wrap these arguments to `array` function
   * Such functions stored in EvalFunctions.varargsFunctions map
   * Key: name of function, value: number of fixed arguments
   *
   * @param ex [[Expression]] - Catalyst expression to check
   * @return [[Expression]] - modified Catalyst expression
   */
  def wrapChildrenToArray(ex: Expression): Expression = {
    val funclist = EvalFunctions.varargsFunctions.keys
    if (funclist.exists(ex.sql.contains)) {
      val (func, ch) = (ex.prettyName, ex.children)
      val exNew = EvalFunctions.varargsFunctions.get(func) match {
        case Some(keep) => if (ch.length < keep) ex
        else UnresolvedFunction(func, ch.slice(0, keep) ++ List(UnresolvedFunction("array", ch.drop(keep), false)), false)
        case _ => ex
      }
      exNew.mapChildren(wrapChildrenToArray)
    } else ex
  }

  // Compose children for CaseWhen function
  def convertSeqToCaseWhen(children: Seq[Expression]) = {
    def iterateTail(accum: List[(Expression, Expression)], tail: Seq[Expression]): List[(Expression, Expression)] = tail.toList match {
      case first :: second :: newtail => iterateTail(accum ++ List((first, second)), newtail)
      case _                          => accum
    }

    iterateTail(List(), children)
  }

  // Replace `case` to `CaseWhen`. Then compose children for `CaseWhen`.
  def convertCaseToCasewhen(schema: StructType)(ex: Expression): Expression = {
    val exNew = if (ex.prettyName == "case"){
      val cw = CaseWhen(convertSeqToCaseWhen(ex.children), None)
      modifyCaseBranches(cw,schema)
    }else ex
    exNew.mapChildren(convertCaseToCasewhen(schema))
  }
  /**
    * Function modified case branches if at least one of them but not all have array type.
    * If not arrayType branches found or all of them have arrayType function returns unchanged value
    *
    * @param c [[Column]] - Column that will be computed based on case function
    * @param schema [[StructType]] - Schema in DDL format
    * @return [[Column]] - Column with modified Catalyst expression
    */
  def modifyCaseBranches(expr: Expression, schema: StructType): Expression = {
    if (! expr.isInstanceOf[CaseWhen]) return expr
    val arrayBranches = expr.asInstanceOf[CaseWhen].branches.foldLeft(List[Expression]()){ (acc, b) => if(isArray(b._2, schema)) acc :+ b._2 else acc }
    if (arrayBranches.size == 0) return expr
    val modfiedBranches = expr.asInstanceOf[CaseWhen].branches.map(x => if (arrayBranches.contains(x._2)) x else  (x._1, CreateArray(Seq(x._2))))
    CaseWhen(modfiedBranches)
  }

  def isArray(expr: Expression, schema: StructType): Boolean = {
    val mutivalRetFuncs = Set("array_sort", "array_join", "array_distinct", "filter", "sequence", "array", "mvindex", "mvzip", "split");
    expr.nodeName match {
      case "Literal" => expr.dataType.typeName == "array"
      case "UnresolvedAttribute" => schema(expr.toString.replaceAll("'", "").replaceAll("`", "")).dataType.typeName == "array"
      case "UnresolvedFunction" if (mutivalRetFuncs.contains(expr.prettyName)) => true
      case _ => false
    }
  }

  // If format string contains %d specifier, convert corresponding column to integer
  def floorColsFormatString(ex: Expression): Expression = {
    val newEx = if (ex.prettyName == "format_string") {
      ex.children.toList match {
        case cHead :: cTail => {
          val newChTail = """%(\.\d+)?[dfs]""".r.findAllIn(cHead.toString.replace("%%", "DD")).toList.zip(cTail).map {
            case (a, b) => if (a == "%d") UnresolvedFunction("floor", List(b), false) else b
          }
          UnresolvedFunction("format_string", cHead :: newChTail, false)
        }
        case _ => ex
      }
    } else ex
    newEx.mapChildren(floorColsFormatString)
  }

  /** 
   * Converts time-format string from OTL format to Java format. Steps:
   * - round literals with single quotes: "date %Y-%m-%d" -> "'date' yyyy-MM-dd"
   * - replace OTL time formatters with Java time formatters
   *  
   * @param ex [[Expression]] - Catalyst expression with original OTL func with replacement strftime -> from_unixtime, strptime -> unix_timestamp
   * @param replaceMap [[Map[String, String]]] - Replacement map of time formatters
   * 
   * @return [[Expression]] - fixed Catalyst expression
   */
  def timeFormatConverter(ex: Expression, replaceMap: Map[String, String]): Expression = {
    /**
     * Rounds literals in time-format string with single quotes.
     * 
     * Example: "date %Y-%m-%dHHH" -> "'date' %Y-%m-%d'HHH'"
     * 
     * Note: It's better to do this conversion not in StringExt but here,
     *       because conversion must be applied only to 'from_unixtime' and 'unix_timestamp' functions
     * 
     * @param timeStr [[String]] - time-format string
     * @param timeFormatters [[Traversable[String]]] - seq of time formatters (to keep them in timeStr)
     * 
     * @return [[String]] - fixed time-format string
     */
    def escapeLiterals(timeStr: String, timeFormatters: Traversable[String]): String = {
      // Replace all time formatters with %%. 
      // If keeping, it will be difficult to distinguish them from literals.
      val strReplacedFormatters = timeStr.replaceAll(
        timeFormatters.map(_.replaceAllLiterally("+", "\\+")).mkString("|"),
        "%%"
      )
      // Find literals with regexp, round them with single quotes by foldLeft with initial string
      // Keeping index and using it within foldLeft is necessary because string becomes longer on each step.
      """[a-zA-Z0-9]+""".r.findAllMatchIn(strReplacedFormatters)
        .map(x => (x.start, x.matched, x.end))
        .zipWithIndex
        .foldLeft(timeStr)
        {
          case (acc, ((start, matched, end), i)) => acc.patch(start + 2*i, s"'${matched}'", end-start)
        }
    }
  
    /**
     * Catalyst tree example:
     * 
     * Expression = UnresolvedFunction(
     *  FunctionIdentifier("from_unixtime", None),
     *  ArrayBuffer(UnresolvedAttribute(List("_time")), Literal(%Y, StringType)),
     *  false
     * )
     */
    if ((ex.nodeName == "UnresolvedFunction") 
      && ((ex.prettyName == "from_unixtime") | (ex.prettyName == "unix_timestamp"))) {
      val newCh = ex.children.toList match {
        case attr :: Literal(format, dtype) :: _ => Seq(
          attr,
          Literal(
            escapeLiterals(format.toString, replaceMap.keys)
              .replaceByMap(replaceMap)
          )
        )
        case _ => ex.children
      }
      UnresolvedFunction(ex.prettyName, newCh, false).mapChildren(x => timeFormatConverter(x, replaceMap))
    } else ex.mapChildren(x => timeFormatConverter(x, replaceMap))
  }

  // If function is "dc" or "distinct_count", replace simple function with AggregateExpression calculating dc.  
  def convertDc(ex: Expression, checkAlias: Boolean = true): Expression = ex match {

    // If alias specified, keep alias and transform only internal function.
    case Alias(aliasEx, name) => Alias(convertDc(aliasEx, checkAlias=false), name)()

    // If alias is not specified, transform function and force aliasing as "dc(col)" or "distinct_count(col)".
    // Otherwise column will be aliased as "count(col)" by Spark.
    case UnresolvedFunction(func, children, _) => if (List("dc", "distinct_count").contains(func.funcName)) {
      val agg = AggregateExpression(
        Count(Seq(children.head)),
        Complete,
        true,
        ExprId(0L)
      )
      if (checkAlias) {
        val aliasName = children.head.asInstanceOf[UnresolvedAttribute].name
        Alias(agg, s"${func.funcName}($aliasName)")()
      } else agg
    } else ex
    case _ => ex
  }

  implicit class BetterColumn(c: Column) {
    val tree = c.expr

    def withExtensions(schema: StructType): Column = c
      .withTimeFormatConverter
      .withSmartCase(schema)
      .withSmartFormatString
      .withSmartAdd(schema)
      .withReplaceMaxMin // ! Must be applied before .withWrapToArray
      .withWrapToArray

    def withTimeFormatConverter(): Column = expr(ColumnExt.timeFormatConverter(tree, EvalFunctions.dateFormatOTPToJava).sql)
    def withSmartCase(schema: StructType): Column = {
      expr(ColumnExt.convertCaseToCasewhen(schema)(tree).sql)
      //modifyCaseBranches(ex,schema)
    }
    def withSmartFormatString(): Column = expr(ColumnExt.floorColsFormatString(tree).sql)
    def withSmartAdd(schema: StructType): Column = expr(ColumnExt.changeAddToConcat(tree, schema).sql)
    def withReplaceMaxMin(): Column = expr(ColumnExt.replaceMaxMin(tree).sql)
    def withWrapToArray(): Column = expr(ColumnExt.wrapChildrenToArray(tree).sql)

    // This extension is used only in 'stats' command, rather than in 'eval'.
    // That's why it is not included in 'withExtensions'.
    def withDc(): Column = expr(ColumnExt.convertDc(tree).sql)
  }
}
