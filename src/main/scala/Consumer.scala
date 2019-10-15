package com.github.lsund.chessmovedb_store

import cats.data._
import java.util
import io.circe._, io.circe.generic.auto._
import io.circe.parser._, io.circe.syntax._
import org.apache.kafka.common.errors.WakeupException
import scala.collection.JavaConverters._
import java.util.Properties
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._

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
}

object GameConsumer extends Runnable {
  val xa = Database.transactor()
  val consumer = Consumer.make()

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

  override def run {
    try {
      consumer.subscribe(util.Arrays.asList("game"))
      while (true) {
        val record = consumer.poll(1000).asScala
        for (data <- record.iterator) {
          val message = data.value()
          decodeAndInsert(message, xa)
        }
      }
    } catch {
      case e: WakeupException =>
      // Ignore
    } finally {
      consumer.close()
    }
  }
  def shutdown() {
    consumer.wakeup()
  }
}

object QueryConsumer extends Runnable {

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

  val xa = Database.transactor()
  val consumer = Consumer.make()
  val producer: KafkaProducer[String, String] = makeKafkaProducer()

  def toTuple[A](xs: List[A]): (A, A) = {
    (xs(0), xs(1))
  }
  def produceSuggestion(jsonPlys: String, xa: Database.PostgresTransactor) {
    println(jsonPlys)
    val decodedPlys = decode[List[Ply]](jsonPlys)
    println(decodedPlys)
    decodedPlys match {
      case Left(error) =>
        println("Could not parse Kafka message:" + error)
      case Right(plys) =>
        println("Calculating suggestion...")
        val games = plys.map(x => Database.gamesWithPly(xa, x).toSet)
        val intersection = games.foldLeft(games.head) { (acc, x) =>
          x.toSet.intersect(acc)
        }
        try {
          println("Done.")
          producer.send(
            new ProducerRecord[String, String](
              "suggestion",
              Database
                .nextMoves(
                  xa,
                  intersection.toList,
                  plys
                    .reduceLeft(
                      (t1, t2) =>
                        if (t1.number > t2.number) t1
                        else t2
                    )
                    .number
                )
                .asJson
                .noSpaces
            )
          )
        } catch {
          case e: Exception => {
            e.printStackTrace()
          }
        }
    }
  }

  override def run {
    try {
      consumer.subscribe(util.Arrays.asList("query"))
      while (true) {
        val record = consumer.poll(1000).asScala
        for (data <- record.iterator) {
          val message = data.value()
          produceSuggestion(message, xa)
        }
      }
    } catch {
      case e: WakeupException =>
      // Ignore
    } finally {
      consumer.close()
      producer.close()
    }
  }
  def shutdown() {
    consumer.wakeup()
  }
}
