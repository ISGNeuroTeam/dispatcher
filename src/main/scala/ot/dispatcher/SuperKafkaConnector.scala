package ot.dispatcher

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.WakeupException
import play.api.libs.json.{JsValue, Json}

import java.time.Duration
import java.util
import java.util.Properties
import scala.collection.JavaConverters._

class SuperKafkaConnector() {
  var consumer: KafkaConsumer[String, String] = createConsumer

  var topic: String = _

  var producer: KafkaProducer[String, JsValue] = _

  def this(topic: String) {
    this()
    this.topic = topic
    subscribeConsumer()
    producer = createProducer
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

  private def createProducer(): KafkaProducer[String, JsValue] = {
    val props = new Properties
    props.put("bootstrap.servers", "localhost:9090")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, JsValue](props)
  }

  def sendMessage(topic: String, key: String, value: String): Boolean = {
    try {
      val jsonValue = Json.parse(value)
      val record = new ProducerRecord[String, JsValue](topic, key, jsonValue)
      producer.send(record).get()
      true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        false
      }
    }

  }
}
