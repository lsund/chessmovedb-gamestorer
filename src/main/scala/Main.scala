package com.github.lsund.chessmovedb_store

import com.typesafe.scalalogging._
import java.lang.Runtime

object Main extends App {

  val logger = Logger("chessmovedb-store")
  val xa = Postgres.transactor()
  val gameConsumer = KafkaGameConsumer(xa, logger)
  val queryConsumer = KafkaQueryConsumer(xa, logger)

  new Thread(gameConsumer).start
  new Thread(queryConsumer).start
  val mainThread = Thread.currentThread

  logger.info("Initializing database")
  Postgres.init(xa)

  Runtime.getRuntime
    .addShutdownHook(new Thread() {
      override def run {
        gameConsumer.shutdown
        queryConsumer.shutdown
        try {
          mainThread.join
        } catch {
          case e: InterruptedException =>
            logger.error("Thread interrupted")
        }
      }
    });
}
