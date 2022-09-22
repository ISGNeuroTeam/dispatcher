package ot.dispatcher.kafka.context

import play.api.libs.json.JsValue

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ArrayBuffer

object CommandsContainer {

  var values: ArrayBuffer[JsValue] = ArrayBuffer[JsValue]()

  var syncValues: ConcurrentLinkedQueue[JsValue] = new ConcurrentLinkedQueue[JsValue]()

  

  var changedValues: ArrayBuffer[JsValue] = new ArrayBuffer[JsValue]()
}
