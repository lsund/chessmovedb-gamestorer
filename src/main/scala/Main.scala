package com.github.lsund.gamestorer

import java.util
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import java.util.Properties
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._
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
      topic: String
  ): String = {
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator)
        return data.value()
    }
    return ""
  }

  // val producer = makeKafkaProducer()

  val consumer = makeKafkaConsumer()
  val message = consumeMessage(consumer, "game")
  consumer.close()

  val decodedGame = decode[db.Game](message)

  val db = new Database()

  val xa = db.transactor()

  db.resetDatabase(xa)

  decodedGame match {
    case Left(error) => println("Error")
    case Right(game) =>
      db.insertGame(xa, game)
  }
}
