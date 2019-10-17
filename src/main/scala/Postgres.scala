package com.github.lsund.chessmovedb_store

import scala.concurrent.ExecutionContext
import doobie.implicits._
import doobie.Update0
import doobie.Transactor
import doobie.Update
import doobie.ConnectionIO
import doobie.Fragment
import cats.effect.IO
import cats._
import cats.data._
import cats.implicits._

object Postgres {

  type Transactor = Transactor.Aux[IO, Unit]

  def transactor(): Transactor = {
    implicit val cs = IO.contextShift(ExecutionContext.global)
    return Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      "jdbc:postgresql:chessgame",
      "postgres",
      ""
    )
  }

  def createSql(): Fragment = {
    sql"""
       CREATE TABLE IF NOT EXISTS Turn (
         id           SERIAL PRIMARY KEY,
         gameid       INT NOT NULL,
         number       INT NOT NULL,
         white        TEXT NOT NULL,
         black        TEXT NOT NULL
       );
       CREATE TABLE IF NOT EXISTS Game (
         id           SERIAL PRIMARY KEY,
         lichessid    TEXT NOT NULL,
         winner       TEXT NOT NULL
       );"""
  }

  def dropSql(): Fragment = {
    sql"""
       DROP TABLE IF EXISTS Turn;
       DROP TABLE IF EXISTS Game;
      """
  }

  def reset(xa: Transactor) = {
    (dropSql().update.run, createSql().update.run)
      .mapN(_ + _)
      .transact(xa)
      .unsafeRunSync
  }

  def init(xa: Transactor) {
    createSql().update.run.transact(xa).unsafeRunSync
  }

  def insertTurns(gameid: Int, turns: List[Turn]): ConnectionIO[Int] = {
    val sql =
      """INSERT INTO turn (gameid, number, white, black)
         VALUES (?, ?, ?, ?)"""
    Update[(Int, Int, String, String)](sql)
      .updateMany(turns.map(turn => turn.toTuple(1, turn)))
  }

  def insertGame(xa: Transactor, game: Game) {
    val (id: Int) :: _ =
      sql"""INSERT INTO game (lichessid, winner)
            VALUES (${game.id}, ${game.winner})
            RETURNING id"""
        .query[Int]
        .to[List]
        .transact(xa)
        .unsafeRunSync
        .take(1)
    insertTurns(id, game.turns).transact(xa).unsafeRunSync
  }

  def gamesWithPly(xa: Transactor, ply: Ply): List[Int] = {
    val sql = if (ply.color == "white") {
      sql"""SELECT gameid FROM turn
          WHERE number = ${ply.number}
          AND white = ${ply.move}"""
    } else {
      sql"""SELECT gameid FROM turn
          WHERE number = ${ply.number}
          AND black = ${ply.move}"""
    }
    sql
      .query[Int]
      .to[List]
      .transact(xa)
      .unsafeRunSync
  }

  def gamesWithTurn(xa: Transactor, turn: Turn): List[Int] = {
    sql"""SELECT gameid FROM turn
          WHERE number = ${turn.number}
          AND white = ${turn.white}
          AND black = ${turn.black}"""
      .query[Int]
      .to[List]
      .transact(xa)
      .unsafeRunSync
  }

  def queryTurn(xa: Transactor, id: Int, number: Int): List[Turn] = {
    sql"""SELECT number, white, black
          FROM turn
          WHERE gameid = ${id} AND number = ${number}"""
      .query[Turn]
      .to[List]
      .transact(xa)
      .unsafeRunSync
  }

  def nextPlys(
      xa: Transactor,
      gameids: List[Int],
      plys: List[Ply]
  ): List[Ply] = {
    if (plys.length % 2 == 0) {
      gameids
        .map(id => queryTurn(xa, id, plys.last.number + 1))
        .flatten
        .map(turn => Ply(turn.number, "white", turn.white))
    } else {
      gameids
        .map(id => queryTurn(xa, id, plys.last.number))
        .flatten
        .map(turn => Ply(turn.number, "black", turn.black))
    }
  }
}
