package com.github.lsund.chessmovedb_store

case class Ply(number: Int, color: String, move: String)

case class Turn(number: Int, white: String, black: String) {
  override def toString = s"Turn($number, $white, $black)"

}
