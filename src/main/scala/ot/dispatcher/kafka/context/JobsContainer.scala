package ot.dispatcher.kafka.context

import play.api.libs.json.JsValue

import java.util.concurrent.ConcurrentLinkedQueue

object JobsContainer {
  var syncValues: ConcurrentLinkedQueue[JsValue] = new ConcurrentLinkedQueue[JsValue]()
}
