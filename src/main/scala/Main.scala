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

  def decodeAndInsert(jsonGame: String, xa: Database.PostgresTransactor) {
    val decodedGame = decode[Database.Game](jsonGame)
    decodedGame match {
      case Left(error) =>
        println("Could not parse Kafka message:" + jsonGame)
      case Right(game) =>
        Database.insertGame(xa, game)
        println("Consumed and inserted game")
    }
  }

  def suggest(jsonTurns: String, xa: Database.PostgresTransactor) {
    val turns = decode[List[Database.Turn]](jsonTurns)
    turns match {
      case Left(error) =>
        println("Could not parse Kafka message:" + turns)
      case Right(game) =>
        println(turns)
    }
  }
  object Consumer extends DatabaseTypes {
    def make(): KafkaConsumer[String, String] = {
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

    def consumeMessage(
        consumer: KafkaConsumer[String, String],
        topic: String,
        xa: PostgresTransactor,
        action: (String, PostgresTransactor) => Unit
    ): String = {
      consumer.subscribe(util.Arrays.asList(topic))
      while (true) {
        val record = consumer.poll(1000).asScala
        for (data <- record.iterator) {
          val message = data.value()
          action(message, xa)
        }
      }
      return ""
    }
  }

  // val producer = makeKafkaProducer()

  val xa = Database.transactor()

  val gameConsumer = Consumer.make()
  val gameConsumption = new Thread {
    override def run {
      try {
        Consumer.consumeMessage(gameConsumer, "game", xa, decodeAndInsert)
      } catch {
        case e: WakeupException =>
        // Ignore
      } finally {
        gameConsumer.close()
      }
    }
  }
  val queryConsumer = Consumer.make()
  val queryConsumption = new Thread {
    override def run {
      try {
        Consumer.consumeMessage(queryConsumer, "query", xa, suggest)
      } catch {
        case e: WakeupException =>
        // Ignore
      } finally {
        queryConsumer.close()
      }
    }
  }
  gameConsumption.start
  queryConsumption.start

  val mainThread = Thread.currentThread
  Runtime.getRuntime
    .addShutdownHook(new Thread() {
      override def run {
        gameConsumer.wakeup
        queryConsumer.wakeup
        try {
          mainThread.join
        } catch {
          case e: InterruptedException =>
            println("Thread interrupted")
        }
      }
    });
}
