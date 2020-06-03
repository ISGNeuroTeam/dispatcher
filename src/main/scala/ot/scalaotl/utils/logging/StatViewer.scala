package ot.scalaotl.utils.logging

import org.apache.spark.sql.DataFrame

object StatViewer {
  //   p(instance)('privateMethod)(arg1, arg2, arg3)
  def getPreviewString(_df: DataFrame) : String = new PrivateMethodCaller(_df, "showString")(10,20,false).asInstanceOf[String]

  class PrivateMethodCaller(x: AnyRef, methodName: String) {
    def apply(_args: Any*): Any = {
      val args = _args.map(_.asInstanceOf[AnyRef])

      def _parents: Stream[Class[_]] = Stream(x.getClass) #::: _parents.map(_.getSuperclass)

      val parents = _parents.takeWhile(_ != null).toList
      val methods = parents.flatMap(_.getDeclaredMethods)
      val method = methods.find(_.getName == methodName).getOrElse(throw new IllegalArgumentException("Method " + methodName + " not found"))
      method.setAccessible(true)
      method.invoke(x, args: _*)
    }
  }
}
