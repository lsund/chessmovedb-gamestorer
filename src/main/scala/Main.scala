package com.github.lsund.chessmovedb_gamestorer

import java.lang.Runtime
import java.util
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import java.util.Properties
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.errors.WakeupException
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

  def consumeMessage(
      consumer: KafkaConsumer[String, String],
      topic: String,
      db: Database
  ): String = {
    val xa = db.transactor()
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator) {
        val message = data.value()
        val decodedGame = decode[db.Game](message)
        decodedGame match {
          case Left(error) =>
            println("Could not parse Kafka message:" + message)
          case Right(game) =>
            db.insertGame(xa, game)
            println("Consumed and inserted game")
        }
      }
    }
    return ""
  }

  // val producer = makeKafkaProducer()

  val db = new Database()
  val consumer = makeKafkaConsumer()

  val gameConsumption = new Thread {
    override def run {
      try {
        consumeMessage(consumer, "game", db)
      } catch {
        case e: WakeupException =>
        // Ignore
      } finally {
        consumer.close()
      }
    }
  }
  gameConsumption.start

  val mainThread = Thread.currentThread
  Runtime.getRuntime
    .addShutdownHook(new Thread() {
      override def run {
        consumer.wakeup
        try {
          mainThread.join
        } catch {
          case e: InterruptedException =>
            println("Thread interrupted")
        }
      }
    });
}
