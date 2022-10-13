package ot.dispatcher.kafka.context

case class KafkaMessage(topic: String, key: String, value: String)
