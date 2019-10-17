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
import java.text.ParseException
import com.typesafe.scalalogging._

object KafkaProducer {
  def make(): KafkaProducer[String, String] = {
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
}

object KafkaConsumer {
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

case class KafkaGameConsumer(xa: Postgres.Transactor, logger: Logger)
    extends Runnable {

  val consumer = KafkaConsumer.make()

  // A movestring is a string like "e4 e5 Nf3 Nc6". This function turns
  // this string into a list of Turns
  def moveStringToTurns(moveString: String): List[Turn] = {
    moveString
      .split(" ")
      .grouped(2)
      .toList
      .zip(Stream.from(1))
      .map({
        case (xs, id) =>
          if (xs.length % 2 == 0) {
            Turn(id, xs(0), xs(1))
          } else {
            Turn(id, xs(0), "")
          }
      })
  }

  def decodeAndInsert(
      gameid: String,
      jsonGame: String
  ) {
    val cursor = parse(jsonGame).getOrElse(Json.Null).hcursor
    val winner = cursor.downField("winner").as[String].getOrElse("none")
    val moveString = cursor.downField("moves").as[String]
    (moveString.map(moveStringToTurns)) match {
      case Right(turns) =>
        Postgres.insertGame(xa, Game(gameid, winner, turns))
        logger.info("Done.")
      case _ =>
        logger.error("Could not parse: " + jsonGame)
        logger.error("Game not inserted")
    }
  }

  override def run {
    try {
      consumer.subscribe(util.Arrays.asList("game"))
      while (true) {
        val record = consumer.poll(1000).asScala
        for (data <- record.iterator) {
          val key = data.key()
          logger.info("Inserting game with id: " + key)
          decodeAndInsert(key, data.value())
        }
      }
    } catch {
      case e: WakeupException => // ignore
    } finally {
      consumer.close()
    }
  }

  def shutdown() {
    consumer.wakeup()
  }
}

case class KafkaQueryConsumer(xa: Postgres.Transactor, logger: Logger)
    extends Runnable {

  val consumer = KafkaConsumer.make()
  val producer = KafkaProducer.make()

  def intersectAll[A](sets: List[Set[A]]): List[A] = {
    return if (sets.isEmpty) {
      List.empty
    } else {
      sets
        .foldLeft(sets.head)({ (acc, x) =>
          x.toSet.intersect(acc)
        })
        .toList
    }
  }

  def produceSuggestion(jsonPlys: String) {
    val decodedPlys = decode[List[Ply]](jsonPlys)
    decodedPlys match {
      case Left(error) =>
        logger.info("Could not parse Kafka message:" + error)
      case Right(plys) =>
        logger.info("Calculating suggestion...")
        val games = plys.map(x => Postgres.gamesWithPly(xa, x).toSet)
        try {
          logger.info("Done.")
          producer.send(
            new ProducerRecord[String, String](
              "suggestion",
              Postgres
                .nextPlys(xa, intersectAll(games), plys)
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
          produceSuggestion(message)
        }
      }
    } catch {
      case e: WakeupException => // ignore
    } finally {
      consumer.close()
      producer.close()
    }
  }
  def shutdown() {
    consumer.wakeup()
  }
}
