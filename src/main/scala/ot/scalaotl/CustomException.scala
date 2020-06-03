package ot.scalaotl

import scala.io.Source
import scala.util.{Failure, Success, Try}

class CustomException(errorCode :Integer, searchId:Integer, message: String, msgArgs: List[Any])
  extends Exception(message){
  //(message + ": " + cause.getMessage, cause)
  def this (errorCode :Integer, searchId:Integer, message: String, cause: Throwable, msgArgs: List[Any]) {
    this(errorCode, searchId, message, msgArgs)
    initCause(cause)
  }
  override def getMessage: String = f"[SearchId:$searchId] " + super.getMessage
  override def getLocalizedMessage: String = Try(getErrorDescription(errorCode).format(msgArgs : _*)) match {//"Ошибка запуска процедуры %s : %s"
    case Success(msg) => f"[SearchId:$searchId] "+ msg
    case Failure(_) => f"[SearchId:$searchId] "+ message
  }
  def getErrorDescription(code: Integer) = {
    val strCode = code + ": "
    val dictionary = ot.AppConfig.config.getString("files.log_localisation")
    val file = Source.fromFile(dictionary)
    val res = file.getLines.toList
      .find(l => l.startsWith(strCode)).map(l => l.substring(strCode.length)).getOrElse(message)
    file.close()
    res
  }
}

object CustomException{
  def apply(errorCode: Integer, searchId: Integer, message: String, msgArgs: List[Any] = List()): CustomException =
    new CustomException(errorCode, searchId, message,msgArgs)
  def apply(errorCode: Integer, searchId: Integer, message: String, cause: Throwable, msgArgs: List[Any]): CustomException =
    new CustomException(errorCode, searchId, message,cause , msgArgs)
}
