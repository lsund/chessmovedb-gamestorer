package com.github.lsund.chessmovedb_store

import java.lang.Runtime

object Main extends App {

  val gameConsumer = GameConsumer
  val queryConsumer = QueryConsumer
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
