package com.github.lsund.gamestorer

import java.util
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import java.util.Properties
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._
import scala.collection.JavaConverters._
import doobie.Update0
import doobie.Transactor
import doobie.Update
import doobie.ConnectionIO
import doobie.implicits._
import cats.effect.IO
import scala.concurrent.ExecutionContext
import cats._
import cats.data._
import cats.implicits._

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

  case class Metadata(key: String, value: String) {}

  case class Turn(number: Int, white: String, black: String) {}

  case class Game(metadata: List[Metadata], turns: List[Turn], score: String) {}

  val decodedGame = decode[Game](message)

  def resetDatabase(xa: Transactor.Aux[IO, Unit]) = {
    val drop =
      sql"""
         DROP TABLE IF EXISTS Turn;
         DROP TABLE IF EXISTS Metadata;
         DROP TABLE IF EXISTS Game;
      """.update.run

    val create =
      sql"""
         CREATE TABLE Turn (
           id   SERIAL PRIMARY KEY,
           gameid INT NOT NULL,
           number INT NOT NULL,
           white TEXT NOT NULL,
           black TEXT NOT NULL
         );
         CREATE TABLE Metadata (
           id   SERIAL PRIMARY KEY,
           gameid INT NOT NULL,
           key TEXT NOT NULL,
           value TEXT NOT NULL
         );
         CREATE TABLE Game (
           id   SERIAL PRIMARY KEY,
           score TEXT NOT NULL
         );
      """.update.run
    (drop, create).mapN(_ + _).transact(xa).unsafeRunSync
  }

  def transactor(): Transactor.Aux[IO, Unit] = {
    implicit val cs = IO.contextShift(ExecutionContext.global)
    return Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      "jdbc:postgresql:chessgame",
      "postgres",
      ""
    )
  }

  val xa = transactor()

  val y = xa.yolo
  import y._

  def turnToTuple(gameid: Int, turn: Turn): (Int, Int, String, String) = {
    return (gameid, turn.number, turn.white, turn.black)
  }

  def metadataToTuple(
      gameid: Int,
      metadata: Metadata
  ): (Int, String, String) = {
    (gameid, metadata.key, metadata.value)
  }

  def insertTurns(gameid: Int, turns: List[Turn]): ConnectionIO[Int] = {
    val sql =
      "insert into turn (gameid, number, white, black) values (?, ?, ?, ?)"
    Update[(Int, Int, String, String)](sql)
      .updateMany(turns.map(x => turnToTuple(1, x)))
  }

  def insertMetadata(
      gameid: Int,
      metadata: List[Metadata]
  ): ConnectionIO[Int] = {
    val sql =
      "insert into metadata (gameid, key, value) values (?, ?, ?)"
    Update[(Int, String, String)](sql)
      .updateMany(metadata.map(x => metadataToTuple(1, x)))
  }

  resetDatabase(xa)

  decodedGame match {
    case Left(error) => println("Error")
    case Right(Game(metadata, turns, score)) =>
      val id :: _ = sql"insert into game (score) values ($score) RETURNING id;"
        .query[Int]
        .to[List]
        .transact(xa)
        .unsafeRunSync
        .take(1)
      insertTurns(id, turns).quick.unsafeRunSync
      insertMetadata(id, metadata).quick.unsafeRunSync
  }
}
