package com.github.lsund.gamestorer

import java.util
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

class Consumer {
  def consumeFromKafka(topic: String, limit: Int) = {
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
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] =
      new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator)
        println(data.value())
    }
    consumer.close()
  }
}

object Main extends App {
  val consumer = new Consumer
  consumer.consumeFromKafka("test", 10)
}
