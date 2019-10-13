package com.github.lsund.gamestorer

import java.util
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

object Main extends App {

  def makeKafkaConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    props.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    props.put("group.id", "consumer-group")
    return new KafkaConsumer[String, String](props)
  }

  def consumeMessage(consumer: KafkaConsumer[String, String], topic: String, limit: Int) = {
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator)
        println(data.value())
    }
  }

  val consumer = makeKafkaConsumer()
  consumeMessage(consumer, "game", 10)
  consumer.close()
}
