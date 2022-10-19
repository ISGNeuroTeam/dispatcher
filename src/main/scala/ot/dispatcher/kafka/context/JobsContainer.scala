package ot.dispatcher.kafka.context

import play.api.libs.json.JsValue

import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Contains thread-safe queue for receiving jobs from Kafka broker and processing them in app working cycle
 */
object JobsContainer {
  var syncValues: ConcurrentLinkedQueue[JsValue] = new ConcurrentLinkedQueue[JsValue]()
}
