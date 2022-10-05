package ot.dispatcher

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.WakeupException
import org.apache.log4j.{Level, Logger}
import ot.AppConfig.{config, getLogLevel}
import ot.dispatcher.kafka.context.{JobsContainer, KafkaMessage}
import play.api.libs.json.{JsValue, Json}

import java.time.Duration
import java.util
import java.util.Properties
import scala.collection.JavaConverters._

/**
 * Provides API for immediate interaction with Kafka
 * @param ipAddress - address of node where kafka service hosted
 * @param port - kafka service port
 */
class SuperKafkaConnector(val ipAddress: String, val port: Int) {
  val log: Logger = Logger.getLogger("KafkaConnectorLogger")
  log.setLevel(Level.toLevel(getLogLevel(config, "kafka_connector")))

  var consumer: KafkaConsumer[String, String] = createConsumer
  log.info("Create kafka consumer")

  var producer: KafkaProducer[String, String] = createProducer
  log.info("Create kafka producer")

  private def createConsumer(): KafkaConsumer[String, String] = {

    val props = new Properties
    props.put(s"bootstrap.servers", s"${ipAddress}:${port}")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    new KafkaConsumer[String, String](props)
  }

  private def createProducer(): KafkaProducer[String, String] = {
    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  /**
   * Subscribe to Kafka <node_job> topic and get new jobs for node in real-time
   * @param nodeUuid
   */
  def getNewJobs(nodeUuid: String): Unit = {
    log.info("Jobs getting from Kafka work started")
    subscribeConsumer(nodeUuid + "_job")
    try {
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
        for (record <- records.asScala) {
          val addedValue: JsValue = Json.parse(record.value()).as[JsValue]
          JobsContainer.syncValues.add(addedValue)
          log.info(s"record with key ${record.key()} added to temporary storage")
          println("Receive " + record.value())
        }
      }
    } catch {
      case e: WakeupException => {}
    } finally {
      log.info("Jobs getting from Kafka work finished")
    }
  }

  /**
   * Subscribe Kafka consumer to topic
   * @param topicName - name of topic
   */
  private def subscribeConsumer(topicName: String): Unit = {
    val topic = topicName
    consumer.subscribe(util.Arrays.asList(topic))
    log.info()
  }

  /**
   * Send any message to Kafka
   * @param topic - kafka record topic
   * @param key - kafka record key
   * @param value - kafka record value
   * @return - true if sending was successfull
   */
  def sendMessage(message: KafkaMessage): Unit = {
    val topic = message.topic
    val key = message.key
    val value = message.value
    try {
      val record = new ProducerRecord[String, String](topic, key, value)
      producer.send(record)
      log.info(s"Successfully sending message to kafka with topic ${topic}, key ${key}, ${value}.")
    } catch {
      case e: Exception => {
        log.info(s"Sending message to kafka with topic ${topic}, key ${key}, ${value} was failed")
        log.debug(e.printStackTrace())
      }
    }
  }
}