package com.github.lsund.chessmovedb_store

import java.lang.Runtime

object Main extends App {

  val xa = Database.transactor()
  val gameConsumer = new GameConsumer(xa)
  val queryConsumer = QueryConsumer(xa)
  new Thread(gameConsumer).start
  new Thread(queryConsumer).start
  val mainThread = Thread.currentThread

  println("Initializing database")
  Database.init(xa)

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
