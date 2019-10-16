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

trait DatabaseTypes {
  type PostgresTransactor = Transactor.Aux[IO, Unit]
}

object Database extends DatabaseTypes {

  case class Game(lichessId: String, winner: String, turns: List[Turn])

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

  def reset(xa: PostgresTransactor) = {

    (dropSql().update.run, createSql().update.run)
      .mapN(_ + _)
      .transact(xa)
      .unsafeRunSync
  }

  def init(xa: PostgresTransactor) {
    createSql().update.run.transact(xa).unsafeRunSync
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

  def turnToTuple(gameid: Int, turn: Turn): (Int, Int, String, String) = {
    return (gameid, turn.number, turn.white, turn.black)
  }

  def insertTurns(gameid: Int, turns: List[Turn]): ConnectionIO[Int] = {
    val sql =
      """INSERT INTO turn (gameid, number, white, black)
         VALUES (?, ?, ?, ?)"""
    Update[(Int, Int, String, String)](sql)
      .updateMany(turns.map(x => turnToTuple(1, x)))
  }

  def insertGame(xa: PostgresTransactor, game: Game) {
    val (id: Int) :: _ =
      sql"""INSERT INTO game (lichessid, winner)
            VALUES (${game.lichessId}, ${game.winner})
            RETURNING id"""
        .query[Int]
        .to[List]
        .transact(xa)
        .unsafeRunSync
        .take(1)
    insertTurns(id, game.turns).transact(xa).unsafeRunSync
  }

  def gamesWithPly(xa: PostgresTransactor, ply: Ply): List[Int] = {
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

  def gamesWithTurn(xa: PostgresTransactor, turn: Turn): List[Int] = {
    sql"""SELECT gameid FROM turn
          WHERE number = ${turn.number}
          AND white = ${turn.white}
          AND black = ${turn.black}"""
      .query[Int]
      .to[List]
      .transact(xa)
      .unsafeRunSync
  }

  def turnQuery(xa: PostgresTransactor, id: Int, number: Int): List[Turn] = {
    sql"""SELECT number, white, black
          FROM turn
          WHERE gameid = ${id} AND number = ${number}"""
      .query[Turn]
      .to[List]
      .transact(xa)
      .unsafeRunSync
  }

  def nextPlys(
      xa: PostgresTransactor,
      ids: List[Int],
      plys: List[Ply]
  ): List[Ply] = {
    if (plys.length % 2 == 0) {
      ids
        .map(id => turnQuery(xa, id, plys.last.number + 1))
        .flatten
        .map(turn => Ply(turn.number, "white", turn.white))
    } else {
      ids
        .map(id => turnQuery(xa, id, plys.last.number))
        .flatten
        .map(turn => Ply(turn.number, "black", turn.black))
    }
  }
}
