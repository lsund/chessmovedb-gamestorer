package com.github.lsund.chessmovedb_store

import java.util.Properties
import java.lang.Runtime
import java.util
import org.apache.kafka.clients.producer._

object Main extends App {

  def makeKafkaProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    return new KafkaProducer[String, String](props)
  }

  val producer = makeKafkaProducer()
  val gameConsumer = GameConsumer
  val queryConsumer = new QueryConsumer(makeKafkaProducer())
  new Thread(gameConsumer).start
  new Thread(queryConsumer).start

  val mainThread = Thread.currentThread
  Runtime.getRuntime
    .addShutdownHook(new Thread() {
      override def run {
        gameConsumer.shutdown
        queryConsumer.shutdown
        try {
          mainThread.join
        } catch {
          case e: InterruptedException =>
            println("Thread interrupted")
        }
      }
    });
}
