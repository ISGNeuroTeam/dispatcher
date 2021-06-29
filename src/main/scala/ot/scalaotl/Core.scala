package ot.scalaotl

trait Field {
  def toMap: Map[String, Field] = {
    this match {
      case Keyword(k, v)     => Map(k -> Keyword(k, v))
      case Positional(k, v)  => Map(k -> Positional(k, v))
      case ReturnField(k, v) => Map(k -> ReturnField(k, v))
    }
  }.asInstanceOf[Map[String, Field]]
}

case class Keyword(key: String, value: String) extends Field
case class Positional(sep: String, values: List[String]) extends Field{
  def applyToEachValue(func: String => String) : Positional = this match {case Positional(sep,vals)=> Positional(sep, vals.map(func))}
}
case class StatsEval(newfield: String, expr: String)
case class StatsFunc(newfield: String, func: String, field: String)
case class ReturnField(newfield: String, field: String) extends Field
case class Return(fields: List[ReturnField] = List(), funcs: List[StatsFunc] = List(), evals: List[StatsEval] = List()) {
  val flatFields: List[String] = fields.map(_.field).map(_.stripSuffix("\"").stripPrefix("\""))
  val flatNewFields: List[String] = fields.map(_.newfield)
  def  applyToEachField(func: String => String) : Return = Return(this.fields.map{case ReturnField(f,n)=> ReturnField(func(f),func(n))})
  def modifyEvalExprs(func: String => String): Return = Return(fields, funcs, evals.map{ sev => StatsEval(sev.newfield,func(sev.expr))}
  )
}
