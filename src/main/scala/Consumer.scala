package com.github.lsund.chessmovedb_store

import java.util
import io.circe._, io.circe.generic.auto._
import io.circe.parser._, io.circe.syntax._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.errors.WakeupException
import scala.collection.JavaConverters._
import java.util.Properties

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
  val xa = Database.transactor()
  val consumer = Consumer.make()

  def produceSuggestion(jsonTurns: String, xa: Database.PostgresTransactor) {
    val decodedTurns = decode[List[Database.Turn]](jsonTurns)
    decodedTurns match {
      case Left(error) =>
        println("Could not parse Kafka message:" + error)
      case Right(turns) =>
        println("Printing games with turns" + turns.mkString(" "))
        val games = turns.map(x => Database.gamesWithTurn(xa, x).toSet)
        val intersection = games.foldLeft(games.head) { (acc, x) =>
          x.toSet.intersect(acc)
        }
        def maxNumber(t1: Database.Turn, t2: Database.Turn): Database.Turn = {
          if (t1.number > t2.number) t1 else t2
        }
        Database.nextMoves(
          xa,
          intersection.toList,
          turns.reduceLeft(maxNumber).number
        )
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
    }
  }
  def shutdown() {
    consumer.wakeup()
  }
}
