package ot.dispatcher

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import play.api.libs.json.{JsValue, Json}

import java.time.Duration
import java.util
import java.util.Properties
import scala.collection.JavaConverters._

class SuperKafkaConnector() {
  var consumer: KafkaConsumer[String, String] = createConsumer
  var topic: String = _
  def this(topic: String) {
    this()
    this.topic = topic
    subscribeConsumer()
  }

  private def createConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties
    props.put("bootstrap.servers", "localhost:9090")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    new KafkaConsumer[String, String](props)
  }

  private def subscribeConsumer(): Unit = {
    consumer.subscribe(util.Arrays.asList(topic))
  }

  def getNewCommands(): List[JsValue] = {
    val simpleMovingConnector = new SuperKafkaConnector
    val mainThread = Thread.currentThread
    Runtime.getRuntime.addShutdownHook(new Thread(){
      override def run(): Unit = {
        simpleMovingConnector.consumer.wakeup
        try {
          mainThread.join
        } catch {
          case e: InterruptedException => e.printStackTrace
        }
      }
    })
    val commands = List[JsValue]()
    try {
      simpleMovingConnector.consumer.subscribe(util.Arrays.asList(topic))
      while (true) {
        val records: ConsumerRecords[String, String] = simpleMovingConnector.consumer.poll(Duration.ofMillis(100))
        for (record <- records.asScala) {
          commands :+ Json.parse(record.topic())
        }
        simpleMovingConnector.consumer.commitSync
      }
    } catch {
      case e: WakeupException => return commands
    }finally {
      simpleMovingConnector.consumer.close
    }
    commands
  }
}
