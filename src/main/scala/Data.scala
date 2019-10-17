package com.github.lsund.chessmovedb_store

// A Ply is a move done by one player, a half-turn.
// For example, blacks 3rd move
case class Ply(number: Int, color: String, move: String)

// A turn is one move by white and the concecutive move by black
case class Turn(number: Int, white: String, black: String) {
  override def toString = s"Turn($number, $white, $black)"
}

// Unique id, winner (white or black) and the list of Turns
case class Game(id: String, winner: String, turns: List[Turn])
